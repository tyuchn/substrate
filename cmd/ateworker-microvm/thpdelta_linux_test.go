//go:build linux

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
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func TestSyncTmpfsToBtrfsIncremental(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "ram-mem")
	snap1Path := filepath.Join(dir, "snap1-mem")
	snap2Path := filepath.Join(dir, "snap2-mem")
	crcCache := filepath.Join(dir, ".mem.crc32c")
	snap1Crc := filepath.Join(dir, ".snap1.crc32c")
	snap2Crc := filepath.Join(dir, ".snap2.crc32c")

	const totalSize = 8 << 20 // 8 MiB sparse file
	src, err := os.OpenFile(srcPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Truncate(totalSize); err != nil {
		t.Fatal(err)
	}

	// Write two non-zero 128 KiB regions at 0 and 4 MiB, leaving the rest sparse.
	regionA := bytes.Repeat([]byte("A"), 128<<10)
	regionB := bytes.Repeat([]byte("B"), 128<<10)
	if _, err := src.WriteAt(regionA, 0); err != nil {
		t.Fatal(err)
	}
	if _, err := src.WriteAt(regionB, 4<<20); err != nil {
		t.Fatal(err)
	}
	if err := src.Close(); err != nil {
		t.Fatal(err)
	}

	// Initial sync (no base file): writes only the populated 256 KiB.
	written1, _, err := syncTmpfsToBtrfsIncremental(srcPath, "", snap1Path, crcCache, snap1Crc)
	if err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}
	if written1 != 256<<10 {
		t.Fatalf("initial writtenBytes = %d, want %d", written1, 256<<10)
	}
	want1, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	got1, err := os.ReadFile(snap1Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1, want1) {
		t.Fatal("snap1 contents do not match source")
	}

	// Stage snap1 back to a fresh destination via stageTmpfsHugepageFile and verify exact equality.
	restagedPath := filepath.Join(dir, "restaged-mem")
	if err := stageTmpfsHugepageFile(snap1Path, restagedPath); err != nil {
		t.Fatalf("stageTmpfsHugepageFile failed: %v", err)
	}
	gotRestaged, err := os.ReadFile(restagedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(gotRestaged, want1) {
		t.Fatal("restaged contents do not match snap1")
	}

	// Mutate one 64 KiB chunk at 4 MiB, zero out [64 KiB, 128 KiB), and add a new 64 KiB chunk at 6 MiB.
	src2, err := os.OpenFile(srcPath, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := src2.WriteAt(bytes.Repeat([]byte{0}, 64<<10), 64<<10); err != nil {
		t.Fatal(err)
	}
	if _, err := src2.WriteAt(bytes.Repeat([]byte("C"), 64<<10), 4<<20); err != nil {
		t.Fatal(err)
	}
	if _, err := src2.WriteAt(bytes.Repeat([]byte("D"), 64<<10), 6<<20); err != nil {
		t.Fatal(err)
	}
	if err := src2.Close(); err != nil {
		t.Fatal(err)
	}

	// Copy snap1Path -> snap2Path if FICLONE isn't supported on tmpdir, and test incremental sync.
	// On a non-reflink tmpdir, syncTmpfsToBtrfsIncremental falls back cleanly to full sparse write;
	// verify byte-for-byte fidelity either way.
	_, cloned2, err := syncTmpfsToBtrfsIncremental(srcPath, snap1Path, snap2Path, crcCache, snap2Crc)
	if err != nil {
		t.Fatalf("incremental sync failed: %v", err)
	}
	want2, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	got2, err := os.ReadFile(snap2Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, want2) {
		t.Fatalf("snap2 contents mismatch (cloned=%v)", cloned2)
	}
	// Verify snap1 remained completely untouched!
	got1After, err := os.ReadFile(snap1Path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got1After, want1) {
		t.Fatal("snap1 was mutated during snap2 sync")
	}
}
