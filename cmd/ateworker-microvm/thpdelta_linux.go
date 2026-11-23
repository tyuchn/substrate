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
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"go.ate.dev/substrate/internal/proto/ateworkerpb"
	"golang.org/x/sys/unix"
)

const (
	ramCacheBaseDir       = "/var/lib/ate/ram-cache"
	ramCleanTokenFile     = ".clean-token"
	ramBaseTokenFile      = ".base-token"
	memCrc32cFile         = ".memory-ranges.crc32c"
	deltaChunkSize        = 64 << 10 // 64 KiB per digest chunk
	deltaHalfChunk        = deltaChunkSize / 2
	deltaWriteCoalesceMax = 4 << 20 // 4 MiB max coalesced pwrite
	deltaScanWorkers      = 4
)

var zeroDeltaChunk = make([]byte, deltaChunkSize)

type dataExtent struct {
	start int64
	end   int64
}

func useTmpfsRamCache(actorDirs *ateworkerpb.ActorDirs) bool {
	if !isBtrfsSnapshotDir(actorDirs) {
		return false
	}
	var st unix.Statfs_t
	if err := unix.Statfs(ramCacheBaseDir, &st); err != nil {
		return false
	}
	return st.Type == unix.TMPFS_MAGIC
}

func actorUIDFromSnapshotRoot(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Base(snapshotRootDir(actorDirs))
}

func actorRamCacheDir(actorDirs *ateworkerpb.ActorDirs) string {
	return filepath.Join(ramCacheBaseDir, actorUIDFromSnapshotRoot(actorDirs))
}

func durableCrcFileName(vol string) string {
	return ".durable-" + vol + ".crc32c"
}

func computeChunkSig(buf []byte) uint64 {
	if bytes.Equal(buf, zeroDeltaChunk[:len(buf)]) {
		return 0
	}
	var h1, h2 uint32
	if len(buf) > deltaHalfChunk {
		h1 = crc32.Checksum(buf[:deltaHalfChunk], castagnoliTable)
		h2 = crc32.Checksum(buf[deltaHalfChunk:], castagnoliTable)
	} else {
		h1 = crc32.Checksum(buf, castagnoliTable)
	}
	sig := (uint64(h1) << 32) | uint64(h2)
	if sig == 0 {
		sig = 1
	}
	return sig
}

func listDataExtents(fd int, size int64) ([]dataExtent, error) {
	var extents []dataExtent
	for off := int64(0); off < size; {
		ds, err := unix.Seek(fd, off, unix.SEEK_DATA)
		if err != nil {
			if errors.Is(err, unix.ENXIO) {
				break
			}
			return nil, fmt.Errorf("SEEK_DATA at %d: %w", off, err)
		}
		de, err := unix.Seek(fd, ds, unix.SEEK_HOLE)
		if err != nil {
			return nil, fmt.Errorf("SEEK_HOLE at %d: %w", ds, err)
		}
		if de > ds {
			extents = append(extents, dataExtent{start: ds, end: min(de, size)})
		}
		off = de
	}
	return extents, nil
}

func computeSparseFileSigs(fd int, size int64) ([]uint64, error) {
	numChunks := int((size + deltaChunkSize - 1) / deltaChunkSize)
	sigs := make([]uint64, numChunks)
	if numChunks == 0 {
		return sigs, nil
	}

	extents, err := listDataExtents(fd, size)
	if err != nil {
		return nil, err
	}
	if len(extents) == 0 {
		return sigs, nil
	}

	// Build unique chunk indices covered by non-hole extents.
	var dataChunks []int
	lastAdded := -1
	for _, ext := range extents {
		firstIdx := int(ext.start / deltaChunkSize)
		lastIdx := int((ext.end - 1) / deltaChunkSize)
		if firstIdx <= lastAdded {
			firstIdx = lastAdded + 1
		}
		for idx := firstIdx; idx <= lastIdx && idx < numChunks; idx++ {
			dataChunks = append(dataChunks, idx)
			lastAdded = idx
		}
	}
	if len(dataChunks) == 0 {
		return sigs, nil
	}

	workers := min(deltaScanWorkers, len(dataChunks))
	var wg sync.WaitGroup
	var firstErr error
	var errMu sync.Mutex

	for w := 0; w < workers; w++ {
		startPos := (w * len(dataChunks)) / workers
		endPos := ((w + 1) * len(dataChunks)) / workers
		wg.Add(1)
		go func(slice []int) {
			defer wg.Done()
			buf := make([]byte, deltaChunkSize)
			for _, chunkIdx := range slice {
				off := int64(chunkIdx) * deltaChunkSize
				want := int(min(int64(deltaChunkSize), size-off))
				readBytes := 0
				for readBytes < want {
					n, rerr := unix.Pread(fd, buf[readBytes:want], off+int64(readBytes))
					if rerr != nil {
						if errors.Is(rerr, unix.EINTR) {
							continue
						}
						errMu.Lock()
						if firstErr == nil {
							firstErr = rerr
						}
						errMu.Unlock()
						return
					}
					if n == 0 {
						clear(buf[readBytes:want])
						break
					}
					readBytes += n
				}
				sigs[chunkIdx] = computeChunkSig(buf[:want])
			}
		}(dataChunks[startPos:endPos])
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return sigs, nil
}

func readSigsFile(path string, expectedCount int) ([]uint64, bool) {
	b, err := os.ReadFile(path)
	if err != nil || len(b) != expectedCount*8 {
		return nil, false
	}
	sigs := make([]uint64, expectedCount)
	for i := range sigs {
		sigs[i] = binary.LittleEndian.Uint64(b[i*8 : (i+1)*8])
	}
	return sigs, true
}

func writeSigsFileAtomic(path string, sigs []uint64) error {
	buf := make([]byte, len(sigs)*8)
	for i, v := range sigs {
		binary.LittleEndian.PutUint64(buf[i*8:(i+1)*8], v)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".crc32c-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()
	if _, err := tmp.Write(buf); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

// stageTmpfsHugepageFile copies the sparse data extents of srcPath into dstPath
// on tmpfs through a 2 MiB THP-advised shared mapping so populated 2 MiB
// regions allocate ShmemPmdMapped hugepages while holes stay unallocated.
func stageTmpfsHugepageFile(srcPath, dstPath string) error {
	src, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("opening source file %s: %w", srcPath, err)
	}
	defer src.Close()

	info, err := src.Stat()
	if err != nil {
		return fmt.Errorf("statting source file %s: %w", srcPath, err)
	}
	size := info.Size()

	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	_ = os.Remove(dstPath)
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("creating tmpfs file %s: %w", dstPath, err)
	}
	defer dst.Close()

	if err := dst.Truncate(size); err != nil {
		return fmt.Errorf("sizing tmpfs file %s: %w", dstPath, err)
	}
	if size == 0 {
		return nil
	}

	extents, err := listDataExtents(int(src.Fd()), size)
	if err != nil {
		if _, serr := src.Seek(0, io.SeekStart); serr != nil {
			return serr
		}
		_, cerr := io.Copy(dst, src)
		return cerr
	}
	if len(extents) == 0 {
		return nil
	}

	mapped, mmapErr := unix.Mmap(int(dst.Fd()), 0, int(size), unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if mmapErr == nil {
		_ = unix.Madvise(mapped, unix.MADV_HUGEPAGE)
		defer func() { _ = unix.Munmap(mapped) }()
	}

	srcFd := int(src.Fd())
	buf := make([]byte, 1<<20)
	for _, ext := range extents {
		for pos := ext.start; pos < ext.end; {
			chunk := int(min(ext.end-pos, int64(len(buf))))
			if mmapErr == nil {
				n, rerr := unix.Pread(srcFd, buf[:chunk], pos)
				if rerr != nil {
					if errors.Is(rerr, unix.EINTR) {
						continue
					}
					return fmt.Errorf("reading %s at %d: %w", srcPath, pos, rerr)
				}
				if n == 0 {
					break
				}
				// Skip all-zero 4 KiB blocks so sparse regions inside data extents stay sparse.
				for off := 0; off < n; off += 4096 {
					end := min(off+4096, n)
					if !bytes.Equal(buf[off:end], zeroDeltaChunk[:end-off]) {
						copy(mapped[int(pos)+off:int(pos)+end], buf[off:end])
					}
				}
				pos += int64(n)
				continue
			}
			n, rerr := unix.Pread(srcFd, buf[:chunk], pos)
			if rerr != nil {
				if errors.Is(rerr, unix.EINTR) {
					continue
				}
				return rerr
			}
			if n == 0 {
				break
			}
			if _, werr := dst.WriteAt(buf[:n], pos); werr != nil {
				return werr
			}
			pos += int64(n)
		}
	}
	return nil
}

// syncTmpfsToBtrfsIncremental materializes dstBtrfsPath from srcTmpfsPath by
// reflinking baseBtrfsPath via FICLONE (when available and matching crcCachePath)
// and writing only changed 64 KiB chunks, then kicking off asynchronous
// SYNC_FILE_RANGE_WRITE so dirty extents stream to Glovebox in the background.
func syncTmpfsToBtrfsIncremental(srcTmpfsPath, baseBtrfsPath, dstBtrfsPath, crcCachePath, snapshotCrcOutPath string) (writtenBytes int64, cloned bool, err error) {
	src, err := os.Open(srcTmpfsPath)
	if err != nil {
		return 0, false, fmt.Errorf("opening tmpfs file %s: %w", srcTmpfsPath, err)
	}
	defer src.Close()

	srcInfo, err := src.Stat()
	if err != nil {
		return 0, false, fmt.Errorf("statting tmpfs file %s: %w", srcTmpfsPath, err)
	}
	size := srcInfo.Size()
	srcFd := int(src.Fd())

	newSigs, err := computeSparseFileSigs(srcFd, size)
	if err != nil {
		return 0, false, fmt.Errorf("computing chunk digests for %s: %w", srcTmpfsPath, err)
	}

	if err := os.MkdirAll(filepath.Dir(dstBtrfsPath), 0o755); err != nil {
		return 0, false, err
	}
	_ = os.Remove(dstBtrfsPath)
	dst, err := os.OpenFile(dstBtrfsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, false, fmt.Errorf("creating btrfs snapshot file %s: %w", dstBtrfsPath, err)
	}
	defer dst.Close()
	dstFd := int(dst.Fd())

	var prevSigs []uint64
	if baseBtrfsPath != "" && crcCachePath != "" {
		if loaded, ok := readSigsFile(crcCachePath, len(newSigs)); ok {
			if baseFile, berr := os.Open(baseBtrfsPath); berr == nil {
				if baseInfo, serr := baseFile.Stat(); serr == nil && baseInfo.Size() == size {
					if cerr := unix.IoctlFileClone(dstFd, int(baseFile.Fd())); cerr == nil {
						prevSigs = loaded
						cloned = true
					}
				}
				_ = baseFile.Close()
			}
		}
	}

	if !cloned {
		if err := dst.Truncate(size); err != nil {
			return 0, false, fmt.Errorf("sizing btrfs snapshot file %s: %w", dstBtrfsPath, err)
		}
		prevSigs = make([]uint64, len(newSigs))
	}

	writeBuf := make([]byte, deltaWriteCoalesceMax)
	minDirtyOff := int64(-1)
	maxDirtyEnd := int64(0)

	for i := 0; i < len(newSigs); {
		if newSigs[i] == prevSigs[i] {
			i++
			continue
		}
		if newSigs[i] == 0 {
			// Coalesce contiguous chunks that transitioned from non-zero to zero.
			runStart := i
			for i < len(newSigs) && newSigs[i] == 0 && prevSigs[i] != 0 {
				i++
			}
			holeOff := int64(runStart) * deltaChunkSize
			holeLen := min(int64(i)*deltaChunkSize, size) - holeOff
			if ferr := unix.Fallocate(dstFd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, holeOff, holeLen); ferr != nil {
				for pos := holeOff; pos < holeOff+holeLen; {
					n := int(min(int64(len(zeroDeltaChunk)), holeOff+holeLen-pos))
					if _, werr := dst.WriteAt(zeroDeltaChunk[:n], pos); werr != nil {
						return writtenBytes, cloned, werr
					}
					pos += int64(n)
				}
			}
			continue
		}

		// Coalesce contiguous dirty non-zero chunks up to deltaWriteCoalesceMax.
		runStart := i
		maxChunks := deltaWriteCoalesceMax / deltaChunkSize
		for i < len(newSigs) && i-runStart < maxChunks && newSigs[i] != 0 && newSigs[i] != prevSigs[i] {
			i++
		}
		writeOff := int64(runStart) * deltaChunkSize
		writeLen := int(min(int64(i)*deltaChunkSize, size) - writeOff)

		readSoFar := 0
		for readSoFar < writeLen {
			n, rerr := unix.Pread(srcFd, writeBuf[readSoFar:writeLen], writeOff+int64(readSoFar))
			if rerr != nil {
				if errors.Is(rerr, unix.EINTR) {
					continue
				}
				return writtenBytes, cloned, fmt.Errorf("pread tmpfs %s at %d: %w", srcTmpfsPath, writeOff, rerr)
			}
			if n == 0 {
				clear(writeBuf[readSoFar:writeLen])
				break
			}
			readSoFar += n
		}
		if _, werr := dst.WriteAt(writeBuf[:writeLen], writeOff); werr != nil {
			return writtenBytes, cloned, fmt.Errorf("writing delta to %s at %d: %w", dstBtrfsPath, writeOff, werr)
		}
		writtenBytes += int64(writeLen)
		if minDirtyOff < 0 || writeOff < minDirtyOff {
			minDirtyOff = writeOff
		}
		if writeOff+int64(writeLen) > maxDirtyEnd {
			maxDirtyEnd = writeOff + int64(writeLen)
		}
	}

	// Start asynchronous non-blocking writeback of the dirtied range so the next
	// checkpoint's FICLONE finds ordered extents already persisted on the ZBD.
	if minDirtyOff >= 0 && maxDirtyEnd > minDirtyOff {
		_ = unix.SyncFileRange(dstFd, minDirtyOff, maxDirtyEnd-minDirtyOff, unix.SYNC_FILE_RANGE_WRITE)
	}

	if crcCachePath != "" {
		if err := writeSigsFileAtomic(crcCachePath, newSigs); err != nil {
			return writtenBytes, cloned, fmt.Errorf("saving crc cache %s: %w", crcCachePath, err)
		}
	}
	if snapshotCrcOutPath != "" {
		if err := writeSigsFileAtomic(snapshotCrcOutPath, newSigs); err != nil {
			return writtenBytes, cloned, fmt.Errorf("saving snapshot crc %s: %w", snapshotCrcOutPath, err)
		}
	}
	return writtenBytes, cloned, nil
}

func findBaseSubvolumeByToken(snapshotRoot string, wantToken []byte) string {
	want := bytes.TrimSpace(wantToken)
	if len(want) == 0 {
		return ""
	}
	localRoot := filepath.Join(snapshotRoot, "local-checkpoint")
	entries, err := os.ReadDir(localRoot)
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		cand := filepath.Join(localRoot, entry.Name())
		tok, rerr := os.ReadFile(filepath.Join(cand, btrfsCheckpointTokenFile))
		if rerr == nil && bytes.Equal(bytes.TrimSpace(tok), want) {
			return cand
		}
	}
	return ""
}
