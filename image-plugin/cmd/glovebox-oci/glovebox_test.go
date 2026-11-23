// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	v1 "github.com/google/go-containerregistry/pkg/v1"
	"go.ate.dev/substrate/api/imageplugin/v1"
	"go.ate.dev/substrate/internal/imagepluginclient"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeZBDBackend struct {
	mu          sync.Mutex
	missingGold map[string]bool
	busyClones  map[string]bool
	attached    map[string]string
	goldSeen    []string
	attachCalls int
	detachCalls int
}

func newFakeZBDBackend() *fakeZBDBackend {
	return &fakeZBDBackend{
		missingGold: make(map[string]bool),
		busyClones:  make(map[string]bool),
		attached:    make(map[string]string),
	}
}

func (f *fakeZBDBackend) Ping(context.Context) error { return nil }

func (f *fakeZBDBackend) AttachClone(_ context.Context, goldID, cloneID, mountDir string, _ int) (AttachTimings, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.missingGold[goldID] {
		return AttachTimings{}, fmt.Errorf("%w: %s missing", imagepluginclient.ErrNotSupported, goldID)
	}
	f.attachCalls++
	f.goldSeen = append(f.goldSeen, goldID)
	f.attached[cloneID] = mountDir

	layerDir := filepath.Join(mountDir, "layer0", "fs")
	if err := os.MkdirAll(layerDir, 0o755); err != nil {
		return AttachTimings{}, err
	}
	if err := os.WriteFile(filepath.Join(mountDir, "layer0", "finalized"), nil, 0o644); err != nil {
		return AttachTimings{}, err
	}
	meta := ociMetaFile{
		InstanceID:     "pydata__xarray-4629",
		GoldenDeviceID: goldID,
	}
	cfgBytes, _ := json.Marshal(&v1.Config{WorkingDir: "/testbed", Env: []string{"PATH=/opt/miniconda3/envs/testbed/bin:/usr/bin"}})
	meta.Config = cfgBytes
	rawMeta, _ := json.Marshal(meta)
	if err := os.WriteFile(filepath.Join(mountDir, "oci-meta.json"), rawMeta, 0o644); err != nil {
		return AttachTimings{}, err
	}
	return AttachTimings{VDPAName: cloneID, BlockDev: "/dev/vda", NumZones: 19, TotalMS: 240.0}, nil
}

func (f *fakeZBDBackend) TryDetachClone(_ context.Context, cloneID, _, mountDir string, lazy bool) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !lazy && f.busyClones[cloneID] {
		return true, nil
	}
	f.detachCalls++
	delete(f.attached, cloneID)
	_ = os.RemoveAll(mountDir)
	return false, nil
}

const testDigest = "sha256:9f21a861697a2e00112233445566778899aabbccddeeff001122334455667788"
const testRef = "us-central1-docker.pkg.dev/example-project/ate-images/swebench/xarray@" + testDigest

func TestSharedROModeSingleAttachAcrossConcurrentBurst(t *testing.T) {
	fb := newFakeZBDBackend()
	srv, err := NewGloveboxServer(Config{
		Mode:     ModeSharedRO,
		WorkDir:  t.TempDir(),
		NodeName: "node-a",
		IdleTTL:  0,
		Backend:  fb,
	})
	if err != nil {
		t.Fatalf("NewGloveboxServer: %v", err)
	}
	defer srv.Close()

	cp, err := srv.CanProvide(t.Context(), &imageplugin.CanProvideRequest{})
	if err != nil || !cp.GetCanProvide() {
		t.Fatalf("CanProvide probe = (%v, %v), want CanProvide=true", cp, err)
	}

	const actors = 24
	var wg sync.WaitGroup
	resps := make([]*imageplugin.PrepareImageResponse, actors)
	errs := make([]error, actors)
	for i := range actors {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resps[i], errs[i] = srv.PrepareImage(t.Context(), &imageplugin.PrepareImageRequest{ImageRef: testRef})
		}()
	}
	wg.Wait()

	for i := range actors {
		if errs[i] != nil {
			t.Fatalf("PrepareImage[%d]: %v", i, errs[i])
		}
		if len(resps[i].GetLayerDirs()) != 1 || resps[i].GetLayerDirs()[0] != resps[0].GetLayerDirs()[0] {
			t.Fatalf("PrepareImage[%d] layers = %v, want shared %v", i, resps[i].GetLayerDirs(), resps[0].GetLayerDirs())
		}
	}

	fb.mu.Lock()
	if fb.attachCalls != 1 {
		t.Fatalf("attachCalls = %d, want 1 shared ZBD mount across %d sandboxes", fb.attachCalls, actors)
	}
	fb.mu.Unlock()

	for i := range actors - 1 {
		if _, err := srv.ReleaseImage(t.Context(), &imageplugin.ReleaseImageRequest{ImageRef: testRef}); err != nil {
			t.Fatalf("ReleaseImage[%d]: %v", i, err)
		}
	}
	fb.mu.Lock()
	if fb.detachCalls != 0 {
		t.Fatalf("detachCalls = %d while 1 sandbox still active, want 0", fb.detachCalls)
	}
	fb.mu.Unlock()

	if _, err := srv.ReleaseImage(t.Context(), &imageplugin.ReleaseImageRequest{ImageRef: testRef}); err != nil {
		t.Fatalf("final ReleaseImage: %v", err)
	}
	fb.mu.Lock()
	if fb.detachCalls != 1 {
		t.Fatalf("detachCalls = %d after last sandbox released, want 1", fb.detachCalls)
	}
	fb.mu.Unlock()
}

func TestSharedEROFSModeUsesEROFSGoldenAndProviderName(t *testing.T) {
	fb := newFakeZBDBackend()
	srv, err := NewGloveboxServer(Config{
		Mode:     ModeSharedRO,
		FSType:   FSEROFS,
		WorkDir:  t.TempDir(),
		NodeName: "node-a",
		Backend:  fb,
	})
	if err != nil {
		t.Fatalf("NewGloveboxServer(FSEROFS): %v", err)
	}
	defer srv.Close()

	resp, err := srv.PrepareImage(t.Context(), &imageplugin.PrepareImageRequest{ImageRef: testRef})
	if err != nil {
		t.Fatalf("PrepareImage: %v", err)
	}
	if resp.GetProviderName() != "glovebox-shared-ro-erofs" {
		t.Fatalf("ProviderName = %q, want glovebox-shared-ro-erofs", resp.GetProviderName())
	}
	if !strings.Contains(resp.GetLayerDirs()[0], "shared-erofs") {
		t.Fatalf("LayerDirs = %v, want shared-erofs path", resp.GetLayerDirs())
	}
	fb.mu.Lock()
	defer fb.mu.Unlock()
	if len(fb.goldSeen) != 1 || fb.goldSeen[0] != "vduse-oe-9f21a861697a2e" {
		t.Fatalf("goldSeen = %v, want [vduse-oe-9f21a861697a2e]", fb.goldSeen)
	}
}

func TestRightSizedZoneAutoDiscoveryAndEROFSHeaderHelpers(t *testing.T) {
	msg := "ERROR: Configured geometry (zone_size_sectors=524288, num_zones=128) does not match persisted superblock (zone_size_sectors=524288, num_zones=19) for device vduse-es-1234"
	got, ok := parsePersistedNumZones(msg)
	if !ok || got != 19 {
		t.Fatalf("parsePersistedNumZones = (%d, %v), want (19, true)", got, ok)
	}

	hdr := make([]byte, 4096)
	binary.LittleEndian.PutUint32(hdr[1024:], erofsSuperMagic)
	binary.LittleEndian.PutUint16(hdr[1024+86:], 1)
	binary.LittleEndian.PutUint16(hdr[1024+88:], 9)
	binary.LittleEndian.PutUint32(hdr[9*128+68:], 2048)
	if mb := parseEROFSMetaBytes(hdr); mb != 2048*4096 {
		t.Fatalf("parseEROFSMetaBytes = %d, want %d", mb, 2048*4096)
	}

	root := t.TempDir()
	for _, sub := range []string{"a/b/c", "a/d", "e/f"} {
		if err := os.MkdirAll(filepath.Join(root, sub), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	warmDirectoryDcache(root, 4)

	metaBlocks := uint32(4)
	bootBuf := make([]byte, metaBlocks*4096)
	binary.LittleEndian.PutUint32(bootBuf[1024:], erofsSuperMagic)
	binary.LittleEndian.PutUint32(bootBuf[1024+36:], 1000)
	binary.LittleEndian.PutUint16(bootBuf[1024+86:], 1)
	binary.LittleEndian.PutUint16(bootBuf[1024+88:], 9)
	binary.LittleEndian.PutUint32(bootBuf[9*128+68:], metaBlocks)

	patchedLen, err := patchStandaloneEROFSMetaBoot(bootBuf)
	if err != nil {
		t.Fatalf("patchStandaloneEROFSMetaBoot: %v", err)
	}
	if patchedLen != int64(metaBlocks)*4096 {
		t.Fatalf("patchedLen = %d, want %d", patchedLen, int64(metaBlocks)*4096)
	}
	if gotBlocks := binary.LittleEndian.Uint32(bootBuf[1024+36:]); gotBlocks != metaBlocks {
		t.Fatalf("sb.blocks = %d, want %d", gotBlocks, metaBlocks)
	}
	if gotMapped := binary.LittleEndian.Uint32(bootBuf[9*128+68:]); gotMapped != 0 {
		t.Fatalf("slot.mapped_blkaddr = %d, want 0", gotMapped)
	}

	jsonl := filepath.Join(t.TempDir(), "images.jsonl")
	const line = `{"golden_erofs_device_id":"vduse-oe-583ef721ebfc24","erofs_num_zones":21,"erofs_meta_aligned_bytes":16777216}` + "\n"
	if err := os.WriteFile(jsonl, []byte(line), 0o644); err != nil {
		t.Fatal(err)
	}
	var hb HostVDUSEBackend
	if err := hb.SeedKnownNumZones(jsonl); err != nil {
		t.Fatalf("SeedKnownNumZones: %v", err)
	}
	if gotZones := hb.lookupNumZones("vduse-oe-583ef721ebfc24"); gotZones != 21 {
		t.Fatalf("lookupNumZones = %d, want 21", gotZones)
	}
	if hb.metaByGold["vduse-oe-583ef721ebfc24"] != 16777216 {
		t.Fatalf("metaByGold = %d, want 16777216", hb.metaByGold["vduse-oe-583ef721ebfc24"])
	}
}

func TestPerActorCoWModeSkipsBusyMountsOnOutOfOrderRelease(t *testing.T) {
	fb := newFakeZBDBackend()
	srv, err := NewGloveboxServer(Config{
		Mode:     ModePerActorCoW,
		WorkDir:  t.TempDir(),
		NodeName: "node-a",
		Backend:  fb,
	})
	if err != nil {
		t.Fatalf("NewGloveboxServer: %v", err)
	}
	defer srv.Close()

	r0, err := srv.PrepareImage(t.Context(), &imageplugin.PrepareImageRequest{ImageRef: testRef})
	if err != nil {
		t.Fatalf("PrepareImage 0: %v", err)
	}
	r1, err := srv.PrepareImage(t.Context(), &imageplugin.PrepareImageRequest{ImageRef: testRef})
	if err != nil {
		t.Fatalf("PrepareImage 1: %v", err)
	}
	if r0.GetLayerDirs()[0] == r1.GetLayerDirs()[0] {
		t.Fatalf("per-actor-cow returned identical LayerDirs %q", r0.GetLayerDirs()[0])
	}

	clone0 := filepath.Base(filepath.Dir(r0.GetLayerDirs()[0]))
	clone1 := filepath.Base(filepath.Dir(r1.GetLayerDirs()[0]))

	fb.mu.Lock()
	fb.busyClones[clone0] = true
	fb.mu.Unlock()

	if _, err := srv.ReleaseImage(t.Context(), &imageplugin.ReleaseImageRequest{ImageRef: testRef}); err != nil {
		t.Fatalf("ReleaseImage (actor 1): %v", err)
	}

	fb.mu.Lock()
	_, ok0 := fb.attached[clone0]
	_, ok1 := fb.attached[clone1]
	fb.mu.Unlock()
	if !ok0 || ok1 {
		t.Fatalf("attached state = (clone0=%v, clone1=%v), want (true, false)", ok0, ok1)
	}

	if _, err := srv.ReleaseImage(t.Context(), &imageplugin.ReleaseImageRequest{ImageRef: testRef}); err != nil {
		t.Fatalf("ReleaseImage (actor 0 while briefly busy): %v", err)
	}
	fb.mu.Lock()
	fb.busyClones[clone0] = false
	fb.mu.Unlock()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		fb.mu.Lock()
		left := len(fb.attached)
		fb.mu.Unlock()
		if left == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("clone0 was not reaped after becoming non-busy")
}

func TestMissingGoldenReturnsFailedPreconditionAndCachesDecline(t *testing.T) {
	fb := newFakeZBDBackend()
	goldID, _, _ := goldenDeviceID(testDigest)
	fb.missingGold[goldID] = true

	srv, err := NewGloveboxServer(Config{
		Mode:       ModeSharedRO,
		WorkDir:    t.TempDir(),
		NodeName:   "node-a",
		DeclineTTL: time.Minute,
		Backend:    fb,
	})
	if err != nil {
		t.Fatalf("NewGloveboxServer: %v", err)
	}
	defer srv.Close()

	_, err = srv.PrepareImage(t.Context(), &imageplugin.PrepareImageRequest{ImageRef: testRef})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("PrepareImage err = %v (code=%v), want FailedPrecondition", err, status.Code(err))
	}

	cp, err := srv.CanProvide(t.Context(), &imageplugin.CanProvideRequest{ImageRef: testRef})
	if err != nil || cp.GetCanProvide() {
		t.Fatalf("CanProvide after decline = (%v, %v), want CanProvide=false", cp, err)
	}
}
