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

//go:build linux

package actordirs

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"go.ate.dev/substrate/cmd/atelet/internal/ateletpath"
	"golang.org/x/sys/unix"
)

const (
	gloveboxConfigPath  = "/proc/1/root/etc/ate-glovebox-zbd.json"
	defaultHostPoolSock = "/proc/1/root/tmp/vduse_zbd_pool.sock"
	defaultZoneSizeMB   = 256
	defaultNumZones     = 64
)

type gloveboxZBDConfig struct {
	PoolSocket string `json:"pool_socket"`
	ZoneSizeMB int    `json:"zone_size_mb"`
	NumZones   int    `json:"num_zones"`
	Prefix     string `json:"prefix"`
}

var (
	zbdNextIndex atomic.Uint32
	zbdMu        sync.Mutex
	zbdActorMu   = make(map[string]*sync.Mutex)
)

func lockActorZBD(actorUID string) func() {
	zbdMu.Lock()
	m, ok := zbdActorMu[actorUID]
	if !ok {
		m = &sync.Mutex{}
		zbdActorMu[actorUID] = m
	}
	zbdMu.Unlock()
	m.Lock()
	return m.Unlock
}

func loadGloveboxZBDConfig() (gloveboxZBDConfig, bool) {
	b, err := os.ReadFile(gloveboxConfigPath)
	if err != nil {
		return gloveboxZBDConfig{}, false
	}
	cfg := gloveboxZBDConfig{
		PoolSocket: defaultHostPoolSock,
		ZoneSizeMB: defaultZoneSizeMB,
		NumZones:   defaultNumZones,
		Prefix:     "vduse-act",
	}
	if err := json.Unmarshal(b, &cfg); err != nil {
		return gloveboxZBDConfig{}, false
	}
	if cfg.PoolSocket == "" {
		cfg.PoolSocket = defaultHostPoolSock
	} else if !strings.HasPrefix(cfg.PoolSocket, "/proc/1/root/") {
		cfg.PoolSocket = filepath.Join("/proc/1/root", cfg.PoolSocket)
	}
	if cfg.ZoneSizeMB <= 0 {
		cfg.ZoneSizeMB = defaultZoneSizeMB
	}
	if cfg.NumZones <= 0 {
		cfg.NumZones = defaultNumZones
	}
	if cfg.Prefix == "" {
		cfg.Prefix = "vduse-act"
	}
	return cfg, true
}

// IsGloveboxPerActorZBDEnabled reports whether deterministic per-actor Glovebox
// ZBD mobility is configured on this node.
func IsGloveboxPerActorZBDEnabled() bool {
	_, ok := loadGloveboxZBDConfig()
	return ok
}

// ActorZBDName maps actorUID deterministically to a VDUSE/vDPA device ID within
// the 31-byte Linux vDPA device name limit.
func ActorZBDName(prefix, actorUID string) string {
	if prefix == "" {
		prefix = "vduse-act"
	}
	var sb strings.Builder
	for _, r := range strings.ToLower(actorUID) {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') {
			sb.WriteRune(r)
		}
	}
	clean := sb.String()
	if len(clean) > 12 {
		clean = clean[:12]
	}
	return prefix + "-" + clean
}

func sendPoolCommand(ctx context.Context, sockPath, cmd string) (string, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", sockPath)
	if err != nil {
		return "", fmt.Errorf("dialing %s: %w", sockPath, err)
	}
	defer conn.Close()

	if dl, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(dl)
	} else {
		_ = conn.SetDeadline(time.Now().Add(60 * time.Second))
	}
	if _, err := conn.Write([]byte(cmd + "\n")); err != nil {
		return "", fmt.Errorf("writing command %q: %w", cmd, err)
	}
	sc := bufio.NewScanner(conn)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return "", err
		}
		return "", errors.New("empty response from VDUSE DaemonPool")
	}
	line := strings.TrimSpace(sc.Text())
	if strings.HasPrefix(line, "ERROR") {
		return line, errors.New(line)
	}
	return line, nil
}

func runHostCommand(ctx context.Context, args ...string) (string, error) {
	nsArgs := append([]string{"-t", "1", "-m", "-u", "-i", "-n", "-p", "--"}, args...)
	cmd := exec.CommandContext(ctx, "/usr/bin/nsenter", nsArgs...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Chroot: "/proc/1/root"}
	cmd.Dir = "/"
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func ensureGloveboxActorZBD(actorUID string, cfg gloveboxZBDConfig) error {
	ensureRamCacheTmpfs()

	unlock := lockActorZBD(actorUID)
	defer unlock()

	snapDir := ateletpath.ActorSnapshotPath(actorUID)
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return fmt.Errorf("creating snapshot mountpoint: %w", err)
	}
	mounted, err := isMountPoint(snapDir)
	if err != nil {
		return fmt.Errorf("checking snapshot mountpoint: %w", err)
	}
	if mounted {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tAll := time.Now()
	devID := ActorZBDName(cfg.Prefix, actorUID)
	devIdx := int(100 + (zbdNextIndex.Add(1) % 4000))

	tStart := time.Now()
	startCmd := fmt.Sprintf("START device_index=%d device_id=%s zone_size_mb=%d num_zones=%d",
		devIdx, devID, cfg.ZoneSizeMB, cfg.NumZones)
	startResp, err := sendPoolCommand(ctx, cfg.PoolSocket, startCmd)
	if err != nil {
		return fmt.Errorf("START %s: %w", devID, err)
	}
	dStart := time.Since(tStart)

	vdpaName := devID
	for _, tok := range strings.Fields(startResp) {
		if after, ok := strings.CutPrefix(tok, "socket_path="); ok && after != "" {
			vdpaName = after
		}
	}

	tVdpa := time.Now()
	if _, err := os.Stat(filepath.Join("/proc/1/root/sys/bus/vdpa/devices", vdpaName)); errors.Is(err, os.ErrNotExist) {
		if out, err := runHostCommand(ctx, "/usr/sbin/vdpa", "dev", "add", "name", vdpaName, "mgmtdev", "vduse"); err != nil {
			_, _ = sendPoolCommand(context.Background(), cfg.PoolSocket, "STOP device_id="+devID)
			return fmt.Errorf("vdpa dev add %s: %w (%s)", vdpaName, err, out)
		}
	}

	var blkName string
	sysGlob := filepath.Join("/proc/1/root/sys/bus/vdpa/devices", vdpaName, "virtio*", "block", "*")
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		matches, _ := filepath.Glob(sysGlob)
		if len(matches) > 0 {
			cand := filepath.Base(matches[0])
			if _, serr := os.Stat(filepath.Join("/host/dev", cand)); serr == nil {
				blkName = cand
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	if blkName == "" {
		_, _ = runHostCommand(context.Background(), "/usr/sbin/vdpa", "dev", "del", vdpaName)
		_, _ = sendPoolCommand(context.Background(), cfg.PoolSocket, "STOP device_id="+devID)
		return fmt.Errorf("timed out waiting for block device under %s", vdpaName)
	}
	dVdpa := time.Since(tVdpa)

	hostDevInContainer := filepath.Join("/host/dev", blkName)
	hostDevInHost := filepath.Join("/dev", blkName)

	tMount := time.Now()
	formatted := false
	const mountOpts = "nospace_cache,nodiscard,nodatasum"
	if err := unix.Mount(hostDevInContainer, snapDir, "btrfs", unix.MS_NOATIME|unix.MS_NODIRATIME, mountOpts); err != nil {
		// Unformatted ZBD on initial actor creation: format zoned btrfs with a unique fsid.
		out, mkErr := runHostCommand(ctx, "/usr/sbin/mkfs.btrfs", "-f", "-O", "zoned", "-R", "^free-space-tree", "-m", "single", "-d", "single", hostDevInHost)
		if mkErr != nil {
			_, _ = runHostCommand(context.Background(), "/usr/sbin/vdpa", "dev", "del", vdpaName)
			_, _ = sendPoolCommand(context.Background(), cfg.PoolSocket, "STOP device_id="+devID)
			return fmt.Errorf("mkfs.btrfs on %s failed after mount err (%v): %w (%s)", hostDevInHost, err, mkErr, out)
		}
		formatted = true
		if merr := unix.Mount(hostDevInContainer, snapDir, "btrfs", unix.MS_NOATIME|unix.MS_NODIRATIME, mountOpts); merr != nil {
			_, _ = runHostCommand(context.Background(), "/usr/sbin/vdpa", "dev", "del", vdpaName)
			_, _ = sendPoolCommand(context.Background(), cfg.PoolSocket, "STOP device_id="+devID)
			return fmt.Errorf("mounting formatted btrfs %s on %s: %w", hostDevInContainer, snapDir, merr)
		}
	}
	dMount := time.Since(tMount)

	slog.Info("Attached per-actor Glovebox ZBD",
		"actor_uid", actorUID,
		"device_id", devID,
		"block_dev", hostDevInHost,
		"formatted", formatted,
		"start_ms", dStart.Milliseconds(),
		"vdpa_ms", dVdpa.Milliseconds(),
		"mount_ms", dMount.Milliseconds(),
		"total_ms", time.Since(tAll).Milliseconds(),
	)
	return nil
}

func detachGloveboxActorZBD(actorUID string, cfg gloveboxZBDConfig) error {
	unlock := lockActorZBD(actorUID)
	defer unlock()

	_ = os.RemoveAll(filepath.Join(ramCacheRoot, actorUID))

	tAll := time.Now()
	snapDir := ateletpath.ActorSnapshotPath(actorUID)
	mounted, err := isMountPoint(snapDir)
	if err != nil {
		return fmt.Errorf("checking snapshot mountpoint %s: %w", snapDir, err)
	}
	if mounted {
		if df, oerr := os.Open(snapDir); oerr == nil {
			_ = unix.Syncfs(int(df.Fd()))
			_ = df.Close()
		}
		if uerr := unix.Unmount(snapDir, 0); uerr != nil {
			if lerr := unix.Unmount(snapDir, unix.MNT_DETACH); lerr != nil && !errors.Is(lerr, unix.EINVAL) && !errors.Is(lerr, unix.ENOENT) {
				return fmt.Errorf("unmounting ZBD %s: %w", snapDir, lerr)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	devID := ActorZBDName(cfg.Prefix, actorUID)
	if _, serr := os.Stat(filepath.Join("/proc/1/root/sys/bus/vdpa/devices", devID)); serr == nil {
		if out, derr := runHostCommand(ctx, "/usr/sbin/vdpa", "dev", "del", devID); derr != nil {
			slog.Warn("vdpa dev del returned error during ZBD detach", "device_id", devID, "err", derr, "out", out)
		}
	}

	if _, serr := sendPoolCommand(ctx, cfg.PoolSocket, "STOP device_id="+devID); serr != nil {
		lower := strings.ToLower(serr.Error())
		if !strings.Contains(lower, "not found") && !strings.Contains(lower, "not active") && !strings.Contains(lower, "unknown") {
			return fmt.Errorf("STOP %s: %w", devID, serr)
		}
	}

	slog.Info("Detached per-actor Glovebox ZBD",
		"actor_uid", actorUID,
		"device_id", devID,
		"total_ms", time.Since(tAll).Milliseconds(),
	)
	return nil
}

// DestroyGloveboxActorZBD permanently destroys the actor's GCS Rapid ZBD when
// Terminate deletes the actor's directories.
func DestroyGloveboxActorZBD(actorUID string) {
	cfg, ok := loadGloveboxZBDConfig()
	if !ok {
		return
	}
	unlock := lockActorZBD(actorUID)
	defer unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	devID := ActorZBDName(cfg.Prefix, actorUID)
	_, _ = sendPoolCommand(ctx, cfg.PoolSocket, "DESTROY device_id="+devID)
}
