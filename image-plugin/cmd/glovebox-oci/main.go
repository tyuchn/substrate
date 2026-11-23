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

// Command glovebox-oci runs the out-of-process ImagePlugin gRPC daemon backed
// by preloaded Golden Glovebox Zoned Btrfs or right-sized EROFS block devices in GCS Rapid.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.ate.dev/substrate/image-plugin/pkg/plugin"
)

func main() {
	var (
		listenSocket      = flag.String("listen-socket", plugin.DefaultListenSocket, "Unix domain socket path where ImagePlugin listens")
		poolSocket        = flag.String("pool-socket", DefaultPoolSocket, "Unix domain socket path for vduse-blk-zbd-daemon")
		mode              = flag.String("mode", ModeSharedRO, "ZBD sharing mode: shared-ro or per-actor-cow")
		fsType            = flag.String("fs", FSBtrfs, "Golden ZBD filesystem format: btrfs or erofs")
		rapidBucket       = flag.String("rapid-bucket", "", "GCS Rapid bucket holding Golden ZBD zone_000000.bin objects for parallel EROFS Zone-0 header staging")
		metaBootDir       = flag.String("meta-boot-dir", DefaultMetaBootDir, "Host tmpfs directory for staged Zone-0 EROFS metadata headers")
		workDir           = flag.String("work-dir", DefaultWorkDir, "Host directory under /var/lib/ate where ZBD clones are mounted")
		nodeName          = flag.String("node-name", os.Getenv("NODE_NAME"), "Kubernetes node name used to namespace CoW clone IDs")
		zoneSizeMB        = flag.Int("zone-size-mb", DefaultZoneSizeMB, "Glovebox ZBD zone size in MiB")
		numZones          = flag.Int("num-zones", DefaultNumZones, "Default number of zones per Golden OCI ZBD when not auto-discovered")
		readAheadKB       = flag.Int("read-ahead-kb", DefaultReadAheadKB, "Block and EROFS read_ahead_kb setting")
		imagesJSONL       = flag.String("images-jsonl", "", "Optional JSONL manifest seeding right-sized erofs_num_zones per Golden ZBD")
		idleTTL           = flag.Duration("idle-ttl", 10*time.Minute, "Duration to keep shared-ro ZBD mounted after refCount reaches 0")
		declineTTL        = flag.Duration("decline-ttl", 2*time.Minute, "Duration to cache missing Golden ZBD lookups")
		startupTimeout    = flag.Duration("startup-timeout", 2*time.Minute, "Maximum wait on startup for vduse-blk-zbd-daemon PING")
		drainTimeout      = flag.Duration("drain-timeout", 5*time.Minute, "Maximum wait on SIGTERM for active leases to drain")
		drainCompleteFile = flag.String("drain-complete-file", plugin.DefaultDrainCompleteFile, "Marker file written once draining completes")
		probe             = flag.Bool("probe", false, "Probe the running ImagePlugin socket for readiness and exit")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if *probe {
		if err := plugin.ProbeUnixSocket(ctx, *listenSocket); err != nil {
			slog.Error("ImagePlugin readiness probe failed", slog.String("socket", *listenSocket), slog.Any("err", err))
			os.Exit(1)
		}
		return
	}

	if *nodeName == "" {
		h, _ := os.Hostname()
		*nodeName = h
	}

	backend := &HostVDUSEBackend{
		PoolSocket:  *poolSocket,
		FSType:      *fsType,
		RapidBucket: *rapidBucket,
		MetaBootDir: *metaBootDir,
		ZoneSizeMB:  *zoneSizeMB,
		NumZones:    *numZones,
		ReadAheadKB: *readAheadKB,
	}
	if err := backend.SeedKnownNumZones(*imagesJSONL); err != nil {
		slog.Warn("Failed to seed known Golden ZBD zone counts", slog.String("path", *imagesJSONL), slog.Any("err", err))
	}

	waitDeadline := time.Now().Add(*startupTimeout)
	for {
		if err := backend.Ping(ctx); err == nil {
			break
		} else if time.Now().After(waitDeadline) || ctx.Err() != nil {
			slog.Error("Timed out waiting for vduse-blk-zbd-daemon readiness", slog.String("poolSocket", *poolSocket), slog.Any("err", err))
			os.Exit(1)
		}
		time.Sleep(500 * time.Millisecond)
	}

	srv, err := NewGloveboxServer(Config{
		Mode:              *mode,
		FSType:            *fsType,
		WorkDir:           *workDir,
		NodeName:          *nodeName,
		IdleTTL:           *idleTTL,
		DeclineTTL:        *declineTTL,
		DrainTimeout:      *drainTimeout,
		DrainCompleteFile: *drainCompleteFile,
		Backend:           backend,
	})
	if err != nil {
		slog.Error("Failed to initialize Glovebox OCI server", slog.Any("err", err))
		os.Exit(1)
	}
	defer func() { _ = srv.Close() }()

	if err := ServeGloveboxUnixSocket(ctx, *listenSocket, srv); err != nil {
		slog.Error("Glovebox OCI ImagePlugin exited with error", slog.Any("err", err))
		os.Exit(1)
	}
}
