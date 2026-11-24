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
	"context"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

func TestActorZBDName(t *testing.T) {
	got := ActorZBDName("vduse-act", "7f3a91c0-4b2e-4f10-9a11-001122334455")
	want := "vduse-act-7f3a91c04b2e"
	if got != want {
		t.Fatalf("ActorZBDName() = %q, want %q", got, want)
	}
	if len(got) > 31 {
		t.Fatalf("ActorZBDName() length %d exceeds 31-byte vDPA cap", len(got))
	}
}

func TestSendPoolCommand(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "pool.sock")
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("Listen: %v", err)
	}
	defer lis.Close()

	go func() {
		conn, err := lis.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 256)
		n, _ := conn.Read(buf)
		if strings.HasPrefix(string(buf[:n]), "START ") {
			_, _ = conn.Write([]byte("OK socket_path=vduse-act-7f3a91c04b2e\n"))
		} else {
			_, _ = conn.Write([]byte("ERROR bad command\n"))
		}
	}()

	resp, err := sendPoolCommand(context.Background(), sock, "START device_index=101 device_id=vduse-act-7f3a91c04b2e")
	if err != nil {
		t.Fatalf("sendPoolCommand(START) unexpected err: %v", err)
	}
	if !strings.Contains(resp, "vduse-act-7f3a91c04b2e") {
		t.Fatalf("sendPoolCommand(START) = %q", resp)
	}
}
