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
	"bufio"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"go.ate.dev/substrate/api/imageplugin/v1"
	"go.ate.dev/substrate/internal/imagepluginclient"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	ModeSharedRO    = "shared-ro"
	ModePerActorCoW = "per-actor-cow"

	FSBtrfs = "btrfs"
	FSEROFS = "erofs"

	DefaultPoolSocket  = "/tmp/vduse_zbd_pool.sock"
	DefaultWorkDir     = "/var/lib/ate/image-plugin-cache/glovebox"
	DefaultMetaBootDir = "/run/ate/glovebox-erofs-meta"
	DefaultZoneSizeMB  = 256
	DefaultNumZones    = 128
	DefaultReadAheadKB = 2048

	erofsSuperMagic      = 0xE0F5E1E2
	maxEROFSMetaPrefetch = 64 << 20
)

var persistedZonesRe = regexp.MustCompile(`persisted superblock \([^)]*num_zones=([0-9]+)\)`)

// AttachTimings records phase durations for cloning, starting, attaching, staging Zone-0 metadata, and mounting a ZBD.
type AttachTimings struct {
	VDPAName   string
	BlockDev   string
	NumZones   int
	CloneMS    float64
	StartMS    float64
	VDPAMS     float64
	MetaWarmMS float64
	MountMS    float64
	TotalMS    float64
}

// ZBDBackend abstracts Glovebox VDUSE pool-ctl socket calls and host mount operations.
type ZBDBackend interface {
	Ping(ctx context.Context) error
	AttachClone(ctx context.Context, goldID, cloneID, mountDir string, deviceIndex int) (AttachTimings, error)
	TryDetachClone(ctx context.Context, cloneID, vdpaName, mountDir string, lazy bool) (busy bool, err error)
}

// Config configures the Glovebox OCI ImagePlugin server.
type Config struct {
	Mode              string
	FSType            string
	WorkDir           string
	NodeName          string
	IdleTTL           time.Duration
	DeclineTTL        time.Duration
	DrainTimeout      time.Duration
	DrainCompleteFile string
	Backend           ZBDBackend
	ResolveDigest     func(ctx context.Context, imageRef string, auth *imageplugin.RegistryAuth) (string, error)
}

type ociMetaFile struct {
	InstanceID     string          `json:"instance_id"`
	ImageDigest    string          `json:"image_digest"`
	GoldenDeviceID string          `json:"golden_device_id"`
	Config         json.RawMessage `json:"config"`
}

type sharedMount struct {
	mu         sync.Mutex
	digest     string
	goldID     string
	cloneID    string
	vdpaName   string
	mountDir   string
	layerDir   string
	configJSON []byte
	mounted    bool
	refCount   int
	idleTimer  *time.Timer
}

type actorMount struct {
	cloneID    string
	vdpaName   string
	mountDir   string
	layerDir   string
	configJSON []byte
}

// GloveboxServer implements substrate.imageplugin.v1.ImagePluginServer over Glovebox ZBDs.
type GloveboxServer struct {
	imageplugin.UnimplementedImagePluginServer

	cfg      Config
	nodeTag  string
	nextIdx  atomic.Uint32
	draining atomic.Bool

	mu              sync.Mutex
	sharedByDigest  map[string]*sharedMount
	actorsByDigest  map[string][]*actorMount
	pendingReleases map[string]int
	digestByRef     map[string]string
	declinedUntil   map[string]time.Time

	stopReaper chan struct{}
	reaperDone chan struct{}
}

// NewGloveboxServer validates cfg and constructs a GloveboxServer.
func NewGloveboxServer(cfg Config) (*GloveboxServer, error) {
	if cfg.Mode != ModeSharedRO && cfg.Mode != ModePerActorCoW {
		return nil, fmt.Errorf("unsupported --mode %q (want %q or %q)", cfg.Mode, ModeSharedRO, ModePerActorCoW)
	}
	if cfg.FSType == "" {
		cfg.FSType = FSBtrfs
	}
	if cfg.FSType != FSBtrfs && cfg.FSType != FSEROFS {
		return nil, fmt.Errorf("unsupported --fs %q (want %q or %q)", cfg.FSType, FSBtrfs, FSEROFS)
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir = DefaultWorkDir
	}
	if cfg.DeclineTTL <= 0 {
		cfg.DeclineTTL = 2 * time.Minute
	}
	if cfg.Backend == nil {
		return nil, errors.New("ZBDBackend is required")
	}
	if cfg.ResolveDigest == nil {
		cfg.ResolveDigest = defaultResolveDigest
	}
	nodeSum := sha256.Sum256([]byte(cfg.NodeName))
	nodeTag := hex.EncodeToString(nodeSum[:])[:4]

	s := &GloveboxServer{
		cfg:             cfg,
		nodeTag:         nodeTag,
		sharedByDigest:  make(map[string]*sharedMount),
		actorsByDigest:  make(map[string][]*actorMount),
		pendingReleases: make(map[string]int),
		digestByRef:     make(map[string]string),
		declinedUntil:   make(map[string]time.Time),
		stopReaper:      make(chan struct{}),
		reaperDone:      make(chan struct{}),
	}
	s.nextIdx.Store(300)
	go s.reapLoop()
	return s, nil
}

// ProviderName returns the distinct provider label for telemetry and logs.
func (s *GloveboxServer) ProviderName() string {
	if s.cfg.FSType == FSEROFS {
		return "glovebox-" + s.cfg.Mode + "-erofs"
	}
	return "glovebox-" + s.cfg.Mode
}

func goldenDeviceID(digest string) (string, string, error) {
	return goldenDeviceIDForFS(digest, FSBtrfs)
}

func goldenDeviceIDForFS(digest, fsType string) (string, string, error) {
	hexPart := strings.TrimPrefix(strings.TrimSpace(digest), "sha256:")
	if len(hexPart) < 14 {
		return "", "", fmt.Errorf("invalid sha256 digest %q", digest)
	}
	hex14 := hexPart[:14]
	if fsType == FSEROFS {
		return "vduse-oe-" + hex14, hex14, nil
	}
	return "vduse-og-" + hex14, hex14, nil
}

// CanProvide checks daemon health, layer form compatibility, and negative cache status.
func (s *GloveboxServer) CanProvide(ctx context.Context, req *imageplugin.CanProvideRequest) (*imageplugin.CanProvideResponse, error) {
	state := imageplugin.PluginState_PLUGIN_STATE_SERVING
	if s.draining.Load() {
		if s.cfg.DrainTimeout > 0 {
			state = imageplugin.PluginState_PLUGIN_STATE_DRAINING
		}
		return &imageplugin.CanProvideResponse{
			CanProvide:   false,
			ProviderName: s.ProviderName(),
			State:        state,
		}, nil
	}
	if req.GetForm() == imageplugin.LayerForm_LAYER_FORM_EROFS {
		return &imageplugin.CanProvideResponse{
			CanProvide:   false,
			ProviderName: s.ProviderName(),
			State:        state,
		}, nil
	}
	if err := s.cfg.Backend.Ping(ctx); err != nil {
		slog.Warn("Glovebox DaemonPool ping failed in CanProvide", slog.Any("err", err))
		return &imageplugin.CanProvideResponse{
			CanProvide:   false,
			ProviderName: s.ProviderName(),
			State:        state,
		}, nil
	}
	ref := strings.TrimSpace(req.GetImageRef())
	if ref != "" && s.isDeclined(ref) {
		return &imageplugin.CanProvideResponse{
			CanProvide:   false,
			ProviderName: s.ProviderName(),
			State:        state,
		}, nil
	}
	return &imageplugin.CanProvideResponse{
		CanProvide:   true,
		ProviderName: s.ProviderName(),
		State:        state,
	}, nil
}

// PrepareImage clones, attaches, and mounts the Golden ZBD in either shared-ro or per-actor-cow mode.
func (s *GloveboxServer) PrepareImage(ctx context.Context, req *imageplugin.PrepareImageRequest) (*imageplugin.PrepareImageResponse, error) {
	if s.draining.Load() {
		return nil, status.Error(codes.FailedPrecondition, "glovebox image plugin is draining")
	}
	if req.GetForm() == imageplugin.LayerForm_LAYER_FORM_EROFS {
		return nil, status.Error(codes.FailedPrecondition, "LAYER_FORM_EROFS is not supported by glovebox-oci")
	}
	ref := strings.TrimSpace(req.GetImageRef())
	if ref == "" {
		return nil, status.Error(codes.InvalidArgument, "image_ref is required")
	}

	digest, err := s.resolveImageDigest(ctx, ref, req.GetAuth())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolving digest for %q: %v", ref, err)
	}
	if s.isDeclined(digest) {
		return nil, status.Errorf(codes.FailedPrecondition, "image %s previously declined", digest)
	}

	if s.cfg.Mode == ModeSharedRO {
		return s.prepareSharedRO(ctx, ref, digest)
	}
	return s.preparePerActorCoW(ctx, ref, digest)
}

func (s *GloveboxServer) prepareSharedRO(ctx context.Context, ref, digest string) (*imageplugin.PrepareImageResponse, error) {
	goldID, hex14, err := goldenDeviceIDForFS(digest, s.cfg.FSType)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	subDir := "shared"
	clonePrefix := "vduse-os"
	if s.cfg.FSType == FSEROFS {
		subDir = "shared-erofs"
		clonePrefix = "vduse-es"
	}

	s.mu.Lock()
	entry, ok := s.sharedByDigest[digest]
	if !ok {
		mountDir := filepath.Join(s.cfg.WorkDir, subDir, hex14)
		entry = &sharedMount{
			digest:   digest,
			goldID:   goldID,
			mountDir: mountDir,
			layerDir: filepath.Join(mountDir, "layer0"),
		}
		s.sharedByDigest[digest] = entry
	}
	s.mu.Unlock()

	entry.mu.Lock()
	defer entry.mu.Unlock()

	if entry.idleTimer != nil {
		entry.idleTimer.Stop()
		entry.idleTimer = nil
	}

	if !entry.mounted {
		devIdx := int(s.nextIdx.Add(1))
		entry.cloneID = fmt.Sprintf("%s-%s-%02x-%s", clonePrefix, s.nodeTag, devIdx&0xff, hex14[:8])
		entry.vdpaName = entry.cloneID
		timings, err := s.cfg.Backend.AttachClone(ctx, entry.goldID, entry.cloneID, entry.mountDir, devIdx)
		if err != nil {
			if errors.Is(err, imagepluginclient.ErrNotSupported) {
				s.markDeclined(ref, digest)
				return nil, status.Errorf(codes.FailedPrecondition, "golden ZBD %s unavailable: %v", entry.goldID, err)
			}
			return nil, status.Errorf(codes.Internal, "attaching shared ZBD %s: %v", entry.cloneID, err)
		}
		if timings.VDPAName != "" {
			entry.vdpaName = timings.VDPAName
		}
		cfgBytes, err := readOCIMetadata(entry.mountDir)
		if err != nil {
			_, _ = s.cfg.Backend.TryDetachClone(context.Background(), entry.cloneID, entry.vdpaName, entry.mountDir, true)
			return nil, status.Errorf(codes.Internal, "reading OCI metadata from %s: %v", entry.mountDir, err)
		}
		entry.configJSON = cfgBytes
		entry.mounted = true
		slog.Info("Mounted shared read-only Glovebox OCI ZBD",
			slog.String("mode", s.cfg.Mode),
			slog.String("fs", s.cfg.FSType),
			slog.String("digest", digest),
			slog.String("gold_id", entry.goldID),
			slog.String("clone_id", entry.cloneID),
			slog.String("block_dev", timings.BlockDev),
			slog.Int("num_zones", timings.NumZones),
			slog.Float64("meta_warm_ms", timings.MetaWarmMS),
			slog.Float64("clone_ms", timings.CloneMS),
			slog.Float64("start_ms", timings.StartMS),
			slog.Float64("vdpa_ms", timings.VDPAMS),
			slog.Float64("mount_ms", timings.MountMS),
			slog.Float64("total_ms", timings.TotalMS))
	}

	entry.refCount++
	return &imageplugin.PrepareImageResponse{
		ImageDigest:  digest,
		ConfigJson:   entry.configJSON,
		LayerDirs:    []string{entry.layerDir},
		ProviderName: s.ProviderName(),
	}, nil
}

func (s *GloveboxServer) preparePerActorCoW(ctx context.Context, ref, digest string) (*imageplugin.PrepareImageResponse, error) {
	goldID, _, err := goldenDeviceIDForFS(digest, s.cfg.FSType)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}

	clonePrefix := "vduse-oa"
	if s.cfg.FSType == FSEROFS {
		clonePrefix = "vduse-ea"
	}

	devIdx := int(s.nextIdx.Add(1))
	cloneID := fmt.Sprintf("%s-%s-%04x", clonePrefix, s.nodeTag, devIdx)
	mountDir := filepath.Join(s.cfg.WorkDir, "actors", cloneID)

	timings, err := s.cfg.Backend.AttachClone(ctx, goldID, cloneID, mountDir, devIdx)
	if err != nil {
		if errors.Is(err, imagepluginclient.ErrNotSupported) {
			s.markDeclined(ref, digest)
			return nil, status.Errorf(codes.FailedPrecondition, "golden ZBD %s unavailable: %v", goldID, err)
		}
		return nil, status.Errorf(codes.Internal, "attaching per-actor CoW ZBD %s: %v", cloneID, err)
	}
	vdpaName := cloneID
	if timings.VDPAName != "" {
		vdpaName = timings.VDPAName
	}
	cfgBytes, err := readOCIMetadata(mountDir)
	if err != nil {
		_, _ = s.cfg.Backend.TryDetachClone(context.Background(), cloneID, vdpaName, mountDir, true)
		return nil, status.Errorf(codes.Internal, "reading OCI metadata from %s: %v", mountDir, err)
	}

	am := &actorMount{
		cloneID:    cloneID,
		vdpaName:   vdpaName,
		mountDir:   mountDir,
		layerDir:   filepath.Join(mountDir, "layer0"),
		configJSON: cfgBytes,
	}
	s.mu.Lock()
	s.actorsByDigest[digest] = append(s.actorsByDigest[digest], am)
	s.mu.Unlock()

	slog.Info("Mounted per-actor CoW Glovebox OCI ZBD",
		slog.String("mode", s.cfg.Mode),
		slog.String("fs", s.cfg.FSType),
		slog.String("digest", digest),
		slog.String("gold_id", goldID),
		slog.String("clone_id", cloneID),
		slog.String("block_dev", timings.BlockDev),
		slog.Int("num_zones", timings.NumZones),
		slog.Float64("meta_warm_ms", timings.MetaWarmMS),
		slog.Float64("clone_ms", timings.CloneMS),
		slog.Float64("start_ms", timings.StartMS),
		slog.Float64("vdpa_ms", timings.VDPAMS),
		slog.Float64("mount_ms", timings.MountMS),
		slog.Float64("total_ms", timings.TotalMS))

	return &imageplugin.PrepareImageResponse{
		ImageDigest:  digest,
		ConfigJson:   cfgBytes,
		LayerDirs:    []string{am.layerDir},
		ProviderName: s.ProviderName(),
	}, nil
}

// ReleaseImage decrements the shared mount refCount or reclaims an unmounted actor CoW clone.
func (s *GloveboxServer) ReleaseImage(ctx context.Context, req *imageplugin.ReleaseImageRequest) (*imageplugin.ReleaseImageResponse, error) {
	ref := strings.TrimSpace(req.GetImageRef())
	if ref == "" {
		return &imageplugin.ReleaseImageResponse{}, nil
	}
	digest, ok := s.lookupCachedDigest(ref)
	if !ok {
		return &imageplugin.ReleaseImageResponse{}, nil
	}

	if s.cfg.Mode == ModeSharedRO {
		s.mu.Lock()
		entry := s.sharedByDigest[digest]
		s.mu.Unlock()
		if entry == nil {
			return &imageplugin.ReleaseImageResponse{}, nil
		}
		entry.mu.Lock()
		defer entry.mu.Unlock()
		if entry.refCount > 0 {
			entry.refCount--
		}
		if entry.refCount == 0 && entry.mounted {
			if s.cfg.IdleTTL <= 0 || s.draining.Load() {
				s.detachSharedLocked(ctx, entry, false)
			} else {
				ttl := s.cfg.IdleTTL
				entry.idleTimer = time.AfterFunc(ttl, func() {
					entry.mu.Lock()
					defer entry.mu.Unlock()
					if entry.refCount == 0 && entry.mounted {
						s.detachSharedLocked(context.Background(), entry, false)
					}
				})
			}
		}
		return &imageplugin.ReleaseImageResponse{}, nil
	}

	s.mu.Lock()
	s.pendingReleases[digest]++
	s.mu.Unlock()
	s.reapActorClones(ctx)
	return &imageplugin.ReleaseImageResponse{}, nil
}

// ReconcileImages rebuilds active reference counts after an atelet restart.
func (s *GloveboxServer) ReconcileImages(_ context.Context, req *imageplugin.ReconcileImagesRequest) (*imageplugin.ReconcileImagesResponse, error) {
	if s.cfg.Mode == ModeSharedRO {
		counts := make(map[string]int)
		for _, img := range req.GetActiveImages() {
			if img == nil || img.GetImageDigest() == "" {
				continue
			}
			counts[img.GetImageDigest()] += int(img.GetRefCount())
		}
		s.mu.Lock()
		entries := make([]*sharedMount, 0, len(s.sharedByDigest))
		for _, e := range s.sharedByDigest {
			entries = append(entries, e)
		}
		s.mu.Unlock()

		for _, entry := range entries {
			entry.mu.Lock()
			entry.refCount = counts[entry.digest]
			if entry.refCount == 0 && entry.mounted && s.cfg.IdleTTL <= 0 {
				s.detachSharedLocked(context.Background(), entry, false)
			}
			entry.mu.Unlock()
		}
	}
	return &imageplugin.ReconcileImagesResponse{}, nil
}

func (s *GloveboxServer) detachSharedLocked(ctx context.Context, entry *sharedMount, lazy bool) {
	busy, err := s.cfg.Backend.TryDetachClone(ctx, entry.cloneID, entry.vdpaName, entry.mountDir, lazy)
	if err != nil || busy {
		slog.Warn("Shared Glovebox ZBD still busy or failed to detach; keeping mounted",
			slog.String("clone_id", entry.cloneID), slog.Bool("busy", busy), slog.Any("err", err))
		return
	}
	entry.mounted = false
	slog.Info("Detached shared Glovebox OCI ZBD", slog.String("clone_id", entry.cloneID))
}

func (s *GloveboxServer) reapLoop() {
	defer close(s.reaperDone)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.stopReaper:
			return
		case <-ticker.C:
			s.reapActorClones(context.Background())
		}
	}
}

func (s *GloveboxServer) reapActorClones(ctx context.Context) {
	s.mu.Lock()
	var digests []string
	for d, want := range s.pendingReleases {
		if want > 0 && len(s.actorsByDigest[d]) > 0 {
			digests = append(digests, d)
		}
	}
	s.mu.Unlock()

	for _, d := range digests {
		s.mu.Lock()
		want := s.pendingReleases[d]
		candidates := append([]*actorMount(nil), s.actorsByDigest[d]...)
		s.mu.Unlock()
		if want <= 0 || len(candidates) == 0 {
			continue
		}

		reaped := make(map[string]bool)
		for _, cand := range candidates {
			if len(reaped) >= want {
				break
			}
			busy, err := s.cfg.Backend.TryDetachClone(ctx, cand.cloneID, cand.vdpaName, cand.mountDir, false)
			if err != nil || busy {
				continue
			}
			reaped[cand.cloneID] = true
			slog.Info("Detached and destroyed per-actor CoW ZBD",
				slog.String("digest", d), slog.String("clone_id", cand.cloneID))
		}
		if len(reaped) == 0 {
			continue
		}

		s.mu.Lock()
		cur := s.actorsByDigest[d]
		kept := cur[:0]
		for _, m := range cur {
			if !reaped[m.cloneID] {
				kept = append(kept, m)
			}
		}
		s.actorsByDigest[d] = kept
		s.pendingReleases[d] = max(0, s.pendingReleases[d]-len(reaped))
		s.mu.Unlock()
	}
}

// Close enters drain mode, stops background timers, and detaches all clones.
func (s *GloveboxServer) Close() error {
	s.draining.Store(true)
	select {
	case <-s.stopReaper:
	default:
		close(s.stopReaper)
	}
	<-s.reaperDone

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	s.mu.Lock()
	shared := make([]*sharedMount, 0, len(s.sharedByDigest))
	for _, e := range s.sharedByDigest {
		shared = append(shared, e)
	}
	var actors []*actorMount
	for k, list := range s.actorsByDigest {
		actors = append(actors, list...)
		delete(s.actorsByDigest, k)
	}
	s.mu.Unlock()

	for _, entry := range shared {
		entry.mu.Lock()
		if entry.idleTimer != nil {
			entry.idleTimer.Stop()
			entry.idleTimer = nil
		}
		if entry.mounted {
			s.detachSharedLocked(ctx, entry, true)
		}
		entry.mu.Unlock()
	}
	for _, am := range actors {
		_, _ = s.cfg.Backend.TryDetachClone(ctx, am.cloneID, am.vdpaName, am.mountDir, true)
	}
	return nil
}

func (s *GloveboxServer) ActiveLeaseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	total := 0
	for _, e := range s.sharedByDigest {
		e.mu.Lock()
		total += e.refCount
		e.mu.Unlock()
	}
	for _, list := range s.actorsByDigest {
		total += len(list)
	}
	return total
}

func (s *GloveboxServer) resolveImageDigest(ctx context.Context, imageRef string, auth *imageplugin.RegistryAuth) (string, error) {
	if d, ok := extractDigestFromRef(imageRef); ok {
		s.cacheRefDigest(imageRef, d)
		return d, nil
	}
	if d, ok := s.lookupCachedDigest(imageRef); ok {
		return d, nil
	}
	d, err := s.cfg.ResolveDigest(ctx, imageRef, auth)
	if err != nil {
		return "", err
	}
	s.cacheRefDigest(imageRef, d)
	return d, nil
}

func extractDigestFromRef(imageRef string) (string, bool) {
	_, dig, ok := strings.Cut(imageRef, "@")
	if ok && strings.HasPrefix(dig, "sha256:") && len(dig) == len("sha256:")+64 {
		return dig, true
	}
	if strings.HasPrefix(imageRef, "sha256:") && len(imageRef) == len("sha256:")+64 {
		return imageRef, true
	}
	return "", false
}

func (s *GloveboxServer) cacheRefDigest(imageRef, digest string) {
	s.mu.Lock()
	s.digestByRef[imageRef] = digest
	s.digestByRef[digest] = digest
	s.mu.Unlock()
}

func (s *GloveboxServer) lookupCachedDigest(imageRef string) (string, bool) {
	if d, ok := extractDigestFromRef(imageRef); ok {
		return d, true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.digestByRef[imageRef]
	return d, ok
}

func (s *GloveboxServer) markDeclined(ref, digest string) {
	until := time.Now().Add(s.cfg.DeclineTTL)
	s.mu.Lock()
	if ref != "" {
		s.declinedUntil[ref] = until
	}
	if digest != "" {
		s.declinedUntil[digest] = until
	}
	s.mu.Unlock()
}

func (s *GloveboxServer) isDeclined(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if until, ok := s.declinedUntil[key]; ok {
		if time.Now().Before(until) {
			return true
		}
		delete(s.declinedUntil, key)
	}
	if d, ok := extractDigestFromRef(key); ok {
		if until, ok2 := s.declinedUntil[d]; ok2 && time.Now().Before(until) {
			return true
		}
	}
	return false
}

func readOCIMetadata(mountDir string) ([]byte, error) {
	candidates := []string{mountDir, filepath.Join("/proc/1/root", mountDir)}
	for _, base := range candidates {
		finalizedPath := filepath.Join(base, "layer0", "finalized")
		if _, err := os.Stat(finalizedPath); err != nil {
			continue
		}
		metaBytes, err := os.ReadFile(filepath.Join(base, "oci-meta.json"))
		if err != nil {
			continue
		}
		var meta ociMetaFile
		if err := json.Unmarshal(metaBytes, &meta); err != nil {
			return nil, fmt.Errorf("decoding oci-meta.json: %w", err)
		}
		if len(meta.Config) > 0 && string(meta.Config) != "null" {
			return meta.Config, nil
		}
		raw, _ := json.Marshal(&v1.Config{})
		return raw, nil
	}
	return nil, fmt.Errorf("layer0/finalized or oci-meta.json missing under %s", mountDir)
}

func defaultResolveDigest(ctx context.Context, imageRef string, auth *imageplugin.RegistryAuth) (string, error) {
	ref, err := name.ParseReference(imageRef)
	if err != nil {
		return "", err
	}
	opts := []remote.Option{remote.WithContext(ctx)}
	if auth != nil {
		cfg := authn.AuthConfig{
			Username:      auth.GetUsername(),
			Password:      auth.GetPassword(),
			Auth:          auth.GetAuth(),
			IdentityToken: auth.GetIdentityToken(),
			RegistryToken: auth.GetRegistryToken(),
		}
		if cfg != (authn.AuthConfig{}) {
			opts = append(opts, remote.WithAuth(authn.FromConfig(cfg)))
		}
	}
	desc, err := remote.Head(ref, opts...)
	if err != nil {
		return "", err
	}
	return desc.Digest.String(), nil
}

// ServeGloveboxUnixSocket listens on listenSocket and serves srv with graceful SIGTERM draining.
func ServeGloveboxUnixSocket(ctx context.Context, listenSocket string, srv *GloveboxServer) error {
	sockPath := strings.TrimPrefix(listenSocket, "unix://")
	if err := os.MkdirAll(filepath.Dir(sockPath), 0o755); err != nil {
		return fmt.Errorf("creating socket dir for %s: %w", sockPath, err)
	}
	if srv.cfg.DrainCompleteFile != "" {
		_ = os.Remove(srv.cfg.DrainCompleteFile)
		defer func() {
			_ = os.MkdirAll(filepath.Dir(srv.cfg.DrainCompleteFile), 0o755)
			_ = os.WriteFile(srv.cfg.DrainCompleteFile, []byte(time.Now().UTC().Format(time.RFC3339)+"\n"), 0o644)
		}()
	}
	_ = os.Remove(sockPath)

	lis, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", sockPath, err)
	}
	defer func() {
		_ = lis.Close()
		_ = os.Remove(sockPath)
	}()

	grpcServer := grpc.NewServer()
	imageplugin.RegisterImagePluginServer(grpcServer, srv)

	go func() {
		<-ctx.Done()
		srv.draining.Store(true)
		if srv.cfg.DrainTimeout > 0 {
			deadline := time.Now().Add(srv.cfg.DrainTimeout)
			for time.Now().Before(deadline) && srv.ActiveLeaseCount() > 0 {
				time.Sleep(250 * time.Millisecond)
			}
		}
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			grpcServer.Stop()
		}
	}()

	slog.Info("Glovebox OCI ImagePlugin listening",
		slog.String("provider", srv.ProviderName()),
		slog.String("socket", sockPath),
		slog.String("workDir", srv.cfg.WorkDir))
	return grpcServer.Serve(lis)
}

// HostVDUSEBackend talks to vduse-blk-zbd-daemon over Unix socket and mounts Btrfs or EROFS in host PID 1 namespaces.
type HostVDUSEBackend struct {
	PoolSocket  string
	FSType      string
	RapidBucket string
	MetaBootDir string
	ZoneSizeMB  int
	NumZones    int
	ReadAheadKB int
	HTTPClient  *http.Client

	mu          sync.Mutex
	zonesByGold map[string]int
	metaByGold  map[string]int64
	loopByClone map[string]string
	bootByClone map[string]string
	cachedToken string
	tokenExpiry time.Time
}

func parsePersistedNumZones(errMsg string) (int, bool) {
	m := persistedZonesRe.FindStringSubmatch(errMsg)
	if len(m) != 2 {
		return 0, false
	}
	n, err := strconv.Atoi(m[1])
	if err != nil || n <= 0 {
		return 0, false
	}
	return n, true
}

func parseEROFSMetaSlot(header []byte) (mappedBlk uint32, slotOff int, ok bool) {
	if len(header) < 1024+90 {
		return 0, 0, false
	}
	sb := header[1024:]
	if binary.LittleEndian.Uint32(sb[0:4]) != erofsSuperMagic {
		return 0, 0, false
	}
	extraDevs := binary.LittleEndian.Uint16(sb[86:88])
	devtSlotOff := binary.LittleEndian.Uint16(sb[88:90])
	if extraDevs < 1 || devtSlotOff == 0 {
		return 0, 0, false
	}
	slotOff = int(devtSlotOff) * 128
	if len(header) < slotOff+72 {
		return 0, 0, false
	}
	mappedBlk = binary.LittleEndian.Uint32(header[slotOff+68 : slotOff+72])
	if mappedBlk == 0 || int64(mappedBlk)*4096 > maxEROFSMetaPrefetch {
		return 0, 0, false
	}
	return mappedBlk, slotOff, true
}

func parseEROFSMetaBytes(header []byte) int64 {
	mappedBlk, _, ok := parseEROFSMetaSlot(header)
	if !ok {
		return 0
	}
	return int64(mappedBlk) * 4096
}

func patchStandaloneEROFSMetaBoot(buf []byte) (int64, error) {
	mappedBlk, slotOff, ok := parseEROFSMetaSlot(buf)
	if !ok {
		return 0, errors.New("EROFS header lacks valid mapped_blkaddr device slot")
	}
	metaBytes := int64(mappedBlk) * 4096
	if int64(len(buf)) < metaBytes {
		return 0, fmt.Errorf("EROFS header buffer %d shorter than metaBytes %d", len(buf), metaBytes)
	}
	binary.LittleEndian.PutUint32(buf[1024+36:1024+40], mappedBlk)
	binary.LittleEndian.PutUint32(buf[slotOff+68:slotOff+72], 0)
	return metaBytes, nil
}

func (b *HostVDUSEBackend) httpClient() *http.Client {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.HTTPClient != nil {
		return b.HTTPClient
	}
	pool, _ := x509.SystemCertPool()
	if pool == nil {
		pool = x509.NewCertPool()
	}
	for _, p := range []string{"/etc/ssl/certs/ca-certificates.crt", "/proc/1/root/etc/ssl/certs/ca-certificates.crt"} {
		if pem, err := os.ReadFile(p); err == nil && len(pem) > 0 {
			pool.AppendCertsFromPEM(pem)
			break
		}
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
	}
	b.HTTPClient = &http.Client{Transport: tr}
	return b.HTTPClient
}

func (b *HostVDUSEBackend) gceAccessToken(ctx context.Context) (string, error) {
	b.mu.Lock()
	if b.cachedToken != "" && time.Now().Before(b.tokenExpiry) {
		tok := b.cachedToken
		b.mu.Unlock()
		return tok, nil
	}
	b.mu.Unlock()

	hc := b.httpClient()
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet,
		"http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Metadata-Flavor", "Google")
	resp, err := hc.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("metadata token HTTP %d", resp.StatusCode)
	}
	var payload struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return "", err
	}
	ttl := 25 * time.Minute
	if payload.ExpiresIn > 120 {
		ttl = time.Duration(payload.ExpiresIn-60) * time.Second
	}
	b.mu.Lock()
	b.cachedToken = payload.AccessToken
	b.tokenExpiry = time.Now().Add(ttl)
	b.mu.Unlock()
	return payload.AccessToken, nil
}

// stageZone0MetaBoot fetches the contiguous [0, metaBytes) EROFS metadata region directly from
// rapid_devices/<goldID>/zones/zone_000000.bin in GCS Rapid in a single HTTP 206 Range GET
// (concurrently overlapped with CLONE + START + vdpa) and writes it to host /run/ate tmpfs.
func (b *HostVDUSEBackend) stageZone0MetaBoot(ctx context.Context, goldID, cloneID string) (hostBootPath string, metaBytes int64, err error) {
	bucket := strings.TrimPrefix(strings.TrimSpace(b.RapidBucket), "gs://")
	if bucket == "" {
		return "", 0, errors.New("rapid bucket not configured")
	}
	tok, err := b.gceAccessToken(ctx)
	if err != nil {
		return "", 0, err
	}
	hc := b.httpClient()

	b.mu.Lock()
	knownMeta := b.metaByGold[goldID]
	b.mu.Unlock()

	fetchRange := func(start, endIncl int64) ([]byte, error) {
		objEsc := url.PathEscape(fmt.Sprintf("rapid_devices/%s/zones/zone_000000.bin", goldID))
		u := fmt.Sprintf("https://storage.googleapis.com/storage/v1/b/%s/o/%s?alt=media", bucket, objEsc)
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+tok)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, endIncl))
		resp, err := hc.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusPartialContent && resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("GCS Rapid Zone-0 range GET HTTP %d", resp.StatusCode)
		}
		return io.ReadAll(resp.Body)
	}

	var buf []byte
	if knownMeta > 0 {
		buf, err = fetchRange(0, knownMeta-1)
		if err != nil {
			return "", 0, err
		}
	} else {
		hdr, err := fetchRange(0, 8191)
		if err != nil {
			return "", 0, err
		}
		mb := parseEROFSMetaBytes(hdr)
		if mb <= 0 {
			return "", 0, errors.New("unable to parse EROFS mapped_blkaddr from Zone 0")
		}
		knownMeta = mb
		if mb <= int64(len(hdr)) {
			buf = hdr[:mb]
		} else {
			rest, err := fetchRange(int64(len(hdr)), mb-1)
			if err != nil {
				return "", 0, err
			}
			buf = append(hdr, rest...)
		}
	}

	patchedBytes, err := patchStandaloneEROFSMetaBoot(buf)
	if err != nil {
		return "", 0, err
	}

	bootDir := b.MetaBootDir
	if bootDir == "" {
		bootDir = DefaultMetaBootDir
	}
	hostBootPath = filepath.Join(bootDir, cloneID+".boot")
	localPath := filepath.Join("/proc/1/root", hostBootPath)
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return "", 0, err
	}
	if err := os.WriteFile(localPath, buf[:patchedBytes], 0o644); err != nil {
		return "", 0, err
	}
	return hostBootPath, patchedBytes, nil
}

func warmDirectoryDcache(rootDir string, workers int) {
	if workers <= 0 {
		workers = 32
	}
	var pending atomic.Int64
	dirs := make(chan string, 4096)
	done := make(chan struct{})

	enqueue := func(p string) {
		pending.Add(1)
		go func() {
			dirs <- p
		}()
	}

	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				case d := <-dirs:
					entries, err := os.ReadDir(d)
					if err == nil {
						for _, e := range entries {
							if e.IsDir() {
								sub := filepath.Join(d, e.Name())
								select {
								case dirs <- sub:
									pending.Add(1)
								default:
									enqueue(sub)
								}
							}
						}
					}
					if pending.Add(-1) == 0 {
						close(done)
						return
					}
				}
			}
		}()
	}

	pending.Store(1)
	dirs <- rootDir
	<-done
	wg.Wait()
}

// SeedKnownNumZones pre-populates Golden ZBD zone counts and Zone-0 metadata byte lengths from a preloaded JSONL manifest.
func (b *HostVDUSEBackend) SeedKnownNumZones(jsonlPath string) error {
	if jsonlPath == "" {
		return nil
	}
	f, err := os.Open(jsonlPath)
	if err != nil {
		return err
	}
	defer f.Close()

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.zonesByGold == nil {
		b.zonesByGold = make(map[string]int)
	}
	if b.metaByGold == nil {
		b.metaByGold = make(map[string]int64)
	}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		var rec struct {
			GoldenEROFSID string `json:"golden_erofs_device_id"`
			EROFSZones    int    `json:"erofs_num_zones"`
			MetaAligned   int64  `json:"erofs_meta_aligned_bytes"`
		}
		if err := json.Unmarshal([]byte(line), &rec); err == nil && rec.GoldenEROFSID != "" {
			if rec.EROFSZones > 0 {
				b.zonesByGold[rec.GoldenEROFSID] = rec.EROFSZones
			}
			if rec.MetaAligned > 0 {
				b.metaByGold[rec.GoldenEROFSID] = rec.MetaAligned
			}
		}
	}
	return sc.Err()
}

func (b *HostVDUSEBackend) lookupNumZones(goldID string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.zonesByGold != nil {
		if n, ok := b.zonesByGold[goldID]; ok && n > 0 {
			return n
		}
	}
	if b.NumZones > 0 {
		return b.NumZones
	}
	return DefaultNumZones
}

func (b *HostVDUSEBackend) rememberNumZones(goldID string, n int) {
	if n <= 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.zonesByGold == nil {
		b.zonesByGold = make(map[string]int)
	}
	b.zonesByGold[goldID] = n
}

func (b *HostVDUSEBackend) rememberCloneSplitMount(cloneID, loopDev, hostBootPath string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.loopByClone == nil {
		b.loopByClone = make(map[string]string)
	}
	if b.bootByClone == nil {
		b.bootByClone = make(map[string]string)
	}
	if loopDev != "" {
		b.loopByClone[cloneID] = loopDev
	}
	if hostBootPath != "" {
		b.bootByClone[cloneID] = hostBootPath
	}
}

func (b *HostVDUSEBackend) popCloneSplitMount(cloneID string) (loopDev, hostBootPath string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	loopDev = b.loopByClone[cloneID]
	hostBootPath = b.bootByClone[cloneID]
	delete(b.loopByClone, cloneID)
	delete(b.bootByClone, cloneID)
	return loopDev, hostBootPath
}

func (b *HostVDUSEBackend) poolCommand(ctx context.Context, cmd string) (string, error) {
	sock := b.PoolSocket
	if sock == "" {
		sock = DefaultPoolSocket
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", sock)
	if err != nil {
		return "", fmt.Errorf("dialing %s: %w", sock, err)
	}
	defer conn.Close()

	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	} else {
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	}
	if _, err := conn.Write([]byte(cmd + "\n")); err != nil {
		return "", fmt.Errorf("writing command to %s: %w", sock, err)
	}
	sc := bufio.NewScanner(conn)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return "", err
		}
		return "", errors.New("empty response from DaemonPool")
	}
	line := strings.TrimSpace(sc.Text())
	if strings.HasPrefix(line, "ERROR") {
		return line, errors.New(line)
	}
	return line, nil
}

func hostExec(ctx context.Context, args ...string) (string, error) {
	nsArgs := append([]string{"-t", "1", "-m", "-u", "-i", "-n", "-r", "--"}, args...)
	cmd := exec.CommandContext(ctx, "nsenter", nsArgs...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

// Ping verifies that vduse-blk-zbd-daemon responds with PONG.
func (b *HostVDUSEBackend) Ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := b.poolCommand(pingCtx, "PING")
	if err != nil {
		return err
	}
	if resp != "PONG" {
		return fmt.Errorf("unexpected PING response %q", resp)
	}
	return nil
}

// AttachClone creates a zero-copy CoW clone from goldID, binds VDUSE/vDPA, and mounts Btrfs or right-sized EROFS read-only.
func (b *HostVDUSEBackend) AttachClone(ctx context.Context, goldID, cloneID, mountDir string, deviceIndex int) (AttachTimings, error) {
	var t AttachTimings
	tAll := time.Now()
	zoneMB := b.ZoneSizeMB
	if zoneMB <= 0 {
		zoneMB = DefaultZoneSizeMB
	}
	numZones := b.lookupNumZones(goldID)

	type stageRes struct {
		bootPath  string
		metaBytes int64
		durMS     float64
		err       error
	}
	var stageCh chan stageRes
	if b.FSType == FSEROFS && strings.TrimSpace(b.RapidBucket) != "" {
		stageCh = make(chan stageRes, 1)
		go func() {
			ts := time.Now()
			bp, mb, serr := b.stageZone0MetaBoot(ctx, goldID, cloneID)
			stageCh <- stageRes{
				bootPath:  bp,
				metaBytes: mb,
				durMS:     float64(time.Since(ts).Microseconds()) / 1000.0,
				err:       serr,
			}
		}()
	}

	t0 := time.Now()
	cloneCmd := fmt.Sprintf("CLONE source_device_id=%s target_device_id=%s clone_mode=cow", goldID, cloneID)
	if _, err := b.poolCommand(ctx, cloneCmd); err != nil {
		lower := strings.ToLower(err.Error())
		if strings.Contains(lower, "not_found") || strings.Contains(lower, "not found") || strings.Contains(lower, "does not exist") {
			return t, fmt.Errorf("%w: %v", imagepluginclient.ErrNotSupported, err)
		}
		return t, fmt.Errorf("CLONE %s -> %s: %w", goldID, cloneID, err)
	}
	t.CloneMS = float64(time.Since(t0).Microseconds()) / 1000.0

	t1 := time.Now()
	startCmd := fmt.Sprintf("START device_index=%d device_id=%s parent_device_id=%s zone_size_mb=%d num_zones=%d",
		deviceIndex, cloneID, goldID, zoneMB, numZones)
	startResp, err := b.poolCommand(ctx, startCmd)
	if err != nil {
		if wantZones, ok := parsePersistedNumZones(err.Error()); ok && wantZones != numZones {
			numZones = wantZones
			b.rememberNumZones(goldID, numZones)
			retryCmd := fmt.Sprintf("START device_index=%d device_id=%s parent_device_id=%s zone_size_mb=%d num_zones=%d",
				deviceIndex, cloneID, goldID, zoneMB, numZones)
			startResp, err = b.poolCommand(ctx, retryCmd)
		}
	}
	if err != nil {
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("DESTROY device_id=%s", cloneID))
		return t, fmt.Errorf("START %s: %w", cloneID, err)
	}
	t.NumZones = numZones
	t.StartMS = float64(time.Since(t1).Microseconds()) / 1000.0

	vdpaName := cloneID
	for _, tok := range strings.Fields(startResp) {
		if after, ok := strings.CutPrefix(tok, "socket_path="); ok && after != "" {
			vdpaName = after
		}
	}
	t.VDPAName = vdpaName

	t2 := time.Now()
	if out, err := hostExec(ctx, "vdpa", "dev", "add", "name", vdpaName, "mgmtdev", "vduse"); err != nil {
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("STOP device_id=%s", cloneID))
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("DESTROY device_id=%s", cloneID))
		return t, fmt.Errorf("vdpa dev add %s: %w (%s)", vdpaName, err, out)
	}

	var blkDev, blkName string
	sysGlob := filepath.Join("/proc/1/root/sys/bus/vdpa/devices", vdpaName, "virtio*", "block", "*")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(sysGlob)
		if len(matches) > 0 {
			name := filepath.Base(matches[0])
			if _, err := os.Stat(filepath.Join("/proc/1/root/dev", name)); err == nil {
				blkName = name
				blkDev = "/dev/" + name
				break
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	if blkDev == "" {
		_, _ = hostExec(context.Background(), "vdpa", "dev", "del", vdpaName)
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("STOP device_id=%s", cloneID))
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("DESTROY device_id=%s", cloneID))
		return t, fmt.Errorf("timed out waiting for virtio-blk device for %s", vdpaName)
	}
	t.BlockDev = blkDev
	t.VDPAMS = float64(time.Since(t2).Microseconds()) / 1000.0

	raKB := b.ReadAheadKB
	if raKB <= 0 {
		raKB = DefaultReadAheadKB
	}
	_ = os.WriteFile(filepath.Join("/proc/1/root/sys/block", blkName, "queue", "read_ahead_kb"), []byte(strconv.Itoa(raKB)), 0o644)

	var stagedBoot string
	var stagedMetaBytes int64
	if stageCh != nil {
		sr := <-stageCh
		if sr.err != nil {
			slog.Warn("Zone-0 EROFS metadata range GET failed; falling back to direct block mount",
				slog.String("gold_id", goldID), slog.Any("err", sr.err))
		} else {
			stagedBoot = sr.bootPath
			stagedMetaBytes = sr.metaBytes
			t.MetaWarmMS = sr.durMS
		}
	}

	t3 := time.Now()
	if out, err := hostExec(ctx, "mkdir", "-p", mountDir); err != nil {
		return t, fmt.Errorf("mkdir %s: %w (%s)", mountDir, err, out)
	}
	var mountArgs []string
	var loopDev string
	if b.FSType == FSEROFS && stagedBoot != "" && stagedMetaBytes > 0 {
		ldOut, lerr := hostExec(ctx, "losetup", "--find", "--show", "--read-only", "--offset", strconv.FormatInt(stagedMetaBytes, 10), blkDev)
		if lerr == nil && strings.HasPrefix(ldOut, "/dev/loop") {
			loopDev = ldOut
			mountArgs = []string{"mount", "-t", "erofs", "-o", "ro,loop,device=" + loopDev, stagedBoot, mountDir}
		} else {
			slog.Warn("losetup --offset failed; falling back to direct block mount", slog.Any("err", lerr), slog.String("out", ldOut))
			_ = os.Remove(filepath.Join("/proc/1/root", stagedBoot))
			stagedBoot = ""
			mountArgs = []string{"mount", "-t", "erofs", "-o", "ro", blkDev, mountDir}
		}
	} else if b.FSType == FSEROFS {
		mountArgs = []string{"mount", "-t", "erofs", "-o", "ro", blkDev, mountDir}
	} else {
		mountArgs = []string{"mount", "-t", "btrfs", "-o", "ro,noatime,nodiratime,nospace_cache,nodiscard,nodatasum", blkDev, mountDir}
	}
	if out, err := hostExec(ctx, mountArgs...); err != nil {
		if loopDev != "" {
			_, _ = hostExec(context.Background(), "losetup", "-d", loopDev)
		}
		if stagedBoot != "" {
			_ = os.Remove(filepath.Join("/proc/1/root", stagedBoot))
		}
		_, _ = hostExec(context.Background(), "vdpa", "dev", "del", vdpaName)
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("STOP device_id=%s", cloneID))
		_, _ = b.poolCommand(context.Background(), fmt.Sprintf("DESTROY device_id=%s", cloneID))
		return t, fmt.Errorf("mount %s at %s: %w (%s)", blkDev, mountDir, err, out)
	}
	if loopDev != "" || stagedBoot != "" {
		b.rememberCloneSplitMount(cloneID, loopDev, stagedBoot)
	}
	t.MountMS = float64(time.Since(t3).Microseconds()) / 1000.0

	if b.FSType == FSEROFS {
		if bdiPaths, err := filepath.Glob("/proc/1/root/sys/class/bdi/erofs-*/read_ahead_kb"); err == nil {
			raStr := []byte(strconv.Itoa(raKB))
			for _, p := range bdiPaths {
				_ = os.WriteFile(p, raStr, 0o644)
			}
		}
		if stagedBoot != "" {
			tWarm := time.Now()
			hostRootFS := filepath.Join("/proc/1/root", mountDir, "layer0", "fs")
			if _, err := os.Stat(hostRootFS); err == nil {
				warmDirectoryDcache(hostRootFS, 32)
			}
			t.MetaWarmMS += float64(time.Since(tWarm).Microseconds()) / 1000.0
		}
	}

	t.TotalMS = float64(time.Since(tAll).Microseconds()) / 1000.0
	return t, nil
}

// TryDetachClone unmounts mountDir (returning busy=true if EBUSY and !lazy), removes loop/vdpa, stops, and destroys cloneID.
func (b *HostVDUSEBackend) TryDetachClone(ctx context.Context, cloneID, vdpaName, mountDir string, lazy bool) (bool, error) {
	if vdpaName == "" {
		vdpaName = cloneID
	}
	umountArgs := []string{"umount"}
	if lazy {
		umountArgs = append(umountArgs, "-l")
	}
	umountArgs = append(umountArgs, mountDir)
	if out, err := hostExec(ctx, umountArgs...); err != nil {
		lower := strings.ToLower(out)
		if !lazy && strings.Contains(lower, "busy") {
			return true, nil
		}
		if !strings.Contains(lower, "not mounted") && !strings.Contains(lower, "no mount point") {
			return false, fmt.Errorf("umount %s: %w (%s)", mountDir, err, out)
		}
	}
	loopDev, bootPath := b.popCloneSplitMount(cloneID)
	if loopDev != "" {
		_, _ = hostExec(ctx, "losetup", "-d", loopDev)
	}
	if bootPath != "" {
		_ = os.Remove(filepath.Join("/proc/1/root", bootPath))
	}
	_, _ = hostExec(ctx, "vdpa", "dev", "del", vdpaName)
	_, _ = b.poolCommand(ctx, fmt.Sprintf("STOP device_id=%s", cloneID))
	_, _ = b.poolCommand(ctx, fmt.Sprintf("DESTROY device_id=%s", cloneID))
	_, _ = hostExec(ctx, "rmdir", mountDir)
	return false, nil
}
