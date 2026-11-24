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

// snapshot-followup drives cold suspend/resume and 2-node cross-node ResumeActor
// benchmarks against a live Agent Substrate installation, verifying guest data
// integrity and physical block-device read sectors via bench-hostexec.
package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.ate.dev/substrate/internal/atenet"
	"go.ate.dev/substrate/internal/benchmarking/boomer/boomerutil"
	gluttonpb "go.ate.dev/substrate/internal/proto/glutton"
	"go.ate.dev/substrate/pkg/proto/ateapipb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	memLoadKey     = "memload"
	durDirTestFile = "bench-data"
	mibBytes       = 1 << 20
)

type actorExpect struct {
	name            string
	uid             string
	memChecksum     uint32
	singleSha256    string
	sentinelDigests map[int]string
	firstResumeMs   float64
	fillWriteMs     float64
	coldSuspendMs   float64
	sourceNode      string
}

type sampleRow struct {
	Tag              string  `json:"tag"`
	Mode             string  `json:"mode"`
	Workload         string  `json:"workload"`
	CacheTier        string  `json:"cache_tier,omitempty"`
	Regime           string  `json:"regime,omitempty"`
	Batch            int     `json:"batch"`
	Index            int     `json:"index"`
	ActorName        string  `json:"actor_name"`
	ActorUID         string  `json:"actor_uid"`
	SourceNode       string  `json:"source_node,omitempty"`
	DestNode         string  `json:"dest_node,omitempty"`
	FirstResumeMs    float64 `json:"first_resume_ms"`
	FillWriteMs      float64 `json:"fill_write_ms"`
	ColdSuspendMs    float64 `json:"cold_suspend_ms"`
	ResumeMs         float64 `json:"resume_ms"`
	PostResumeReadMs float64 `json:"post_resume_read_ms"`
	UsableTotalMs    float64 `json:"usable_total_ms"`
	Verified         bool    `json:"verified"`
	BatchPhysReadMiB float64 `json:"batch_phys_read_mib,omitempty"`
	ActorPhysReadMiB float64 `json:"actor_phys_read_mib,omitempty"`
	Error            string  `json:"error,omitempty"`
}

type distStats struct {
	N    int     `json:"n"`
	Min  float64 `json:"min_ms"`
	P50  float64 `json:"p50_ms"`
	P90  float64 `json:"p90_ms"`
	P99  float64 `json:"p99_ms"`
	Max  float64 `json:"max_ms"`
	Mean float64 `json:"mean_ms"`
}

type summaryReport struct {
	Tag                  string    `json:"tag"`
	Mode                 string    `json:"mode"`
	Workload             string    `json:"workload"`
	CacheTier            string    `json:"cache_tier,omitempty"`
	Regime               string    `json:"regime,omitempty"`
	TotalSamples         int       `json:"total_samples"`
	VerifiedSamples      int       `json:"verified_samples"`
	FailedSamples        int       `json:"failed_samples"`
	MeanPhysReadMiBActor float64   `json:"mean_phys_read_mib_per_actor"`
	FirstResume          distStats `json:"first_resume"`
	FillWrite            distStats `json:"fill_write"`
	ColdSuspend          distStats `json:"cold_suspend"`
	Resume               distStats `json:"resume"`
	PostResumeRead       distStats `json:"post_resume_read"`
	UsableTotal          distStats `json:"usable_total"`
}

type runner struct {
	ctrl             ateapipb.ControlClient
	httpClient       *http.Client
	routerURL        string
	atespace         string
	templateAtespace string
	durdirTemplate   string
	k8sCfg           *rest.Config
	k8sClient        *kubernetes.Clientset
}

func main() {
	var (
		apiEndpoint       = flag.String("api-endpoint", "k8s:///api.ate-system.svc.cluster.local:443", "ateapi gRPC target.")
		routerURL         = flag.String("router-url", "https://atenet-router.ate-system.svc.cluster.local", "atenet router base URL.")
		atespace          = flag.String("atespace", "benchmark-followup", "Atespace for test actors.")
		templateAtespace  = flag.String("template-atespace", "benchmark-workloads", "Atespace holding ActorTemplates.")
		durdirTemplate    = flag.String("durdir-template", "glutton-durdir-full", "ActorTemplate name for durdir and smallfiles512.")
		tag               = flag.String("tag", "run", "Run identifier tag.")
		mode              = flag.String("mode", "cold-same-node", "Benchmark mode: cold-same-node or cross-node.")
		workload          = flag.String("workload", "mem", "Workload: mem, durdir, or smallfiles512.")
		cacheTier         = flag.String("cache-tier", "cold-storage", "Cache tier for cold-same-node: cold-storage or btrfs-pagecache.")
		regime            = flag.String("regime", "live-async", "Cross-node regime: live-async or pre-detached.")
		batches           = flag.Int("batches", 4, "Number of batches in cold-same-node mode.")
		batchSize         = flag.Int("batch-size", 25, "Actors per batch in cold-same-node mode.")
		resumeConcurrency = flag.Int("resume-concurrency", 1, "Concurrent resumes in cold-same-node or cross-node burst.")
		trials            = flag.Int("trials", 30, "Total trials in cross-node mode.")
		targetNode        = flag.String("target-node", "", "Worker node name in cold-same-node mode.")
		node1             = flag.String("node1", "", "Source node name in cross-node mode.")
		node2             = flag.String("node2", "", "Destination node name in cross-node mode.")
		labelKey          = flag.String("worker-label-key", "bench-node", "Worker selector label key for cross-node pools.")
		node1Label        = flag.String("node1-label", "node-1", "Worker selector label value for node1.")
		node2Label        = flag.String("node2-label", "node-2", "Worker selector label value for node2.")
		diskRegex         = flag.String("disk-regex", `^nvme0n2$`, "Regex matching block device name in /proc/diskstats.")
		dropNode2Cache    = flag.Bool("drop-node2-cache", true, "Run sync + drop_caches on node2 before cross-node resume.")
	)
	flag.Parse()

	conn, ctrl, err := boomerutil.DialControl(*apiEndpoint)
	if err != nil {
		fatalf("dial control: %v", err)
	}
	defer conn.Close()

	tlsCfg, err := boomerutil.RouterTLSConfig()
	if err != nil {
		fatalf("router tls: %v", err)
	}
	httpClient := &http.Client{
		Timeout: 180 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig:     tlsCfg,
			MaxIdleConns:        256,
			MaxIdleConnsPerHost: 256,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	k8sCfg, err := rest.InClusterConfig()
	if err != nil {
		fatalf("in-cluster k8s config: %v", err)
	}
	k8sClient, err := kubernetes.NewForConfig(k8sCfg)
	if err != nil {
		fatalf("k8s client: %v", err)
	}

	r := &runner{
		ctrl:             ctrl,
		httpClient:       httpClient,
		routerURL:        strings.TrimRight(*routerURL, "/"),
		atespace:         *atespace,
		templateAtespace: *templateAtespace,
		durdirTemplate:   *durdirTemplate,
		k8sCfg:           k8sCfg,
		k8sClient:        k8sClient,
	}

	ctx := context.Background()
	if err := r.ensureAtespace(ctx); err != nil {
		fatalf("ensure atespace: %v", err)
	}

	diskRE, err := regexp.Compile(*diskRegex)
	if err != nil {
		fatalf("compile disk-regex: %v", err)
	}

	var rows []sampleRow
	switch *mode {
	case "cold-same-node":
		if *targetNode == "" {
			fatalf("--target-node is required for cold-same-node")
		}
		rows, err = r.runColdSameNode(ctx, *tag, *workload, *cacheTier, *batches, *batchSize, *resumeConcurrency, *targetNode, diskRE)
	case "cross-node":
		if *node1 == "" || *node2 == "" {
			fatalf("--node1 and --node2 are required for cross-node")
		}
		rows, err = r.runCrossNode(ctx, *tag, *workload, *regime, *trials, *resumeConcurrency, *node1, *node2, *labelKey, *node1Label, *node2Label, diskRE, *dropNode2Cache)
	case "guest-write-io":
		rows, err = r.runGuestWriteIO(ctx, *tag, *workload, *batchSize)
	default:
		fatalf("unsupported --mode %q", *mode)
	}
	if err != nil {
		fatalf("benchmark failed: %v", err)
	}

	fmt.Println("=== BEGIN_SAMPLES_JSONL ===")
	for _, row := range rows {
		b, _ := json.Marshal(row)
		fmt.Println(string(b))
	}
	fmt.Println("=== END_SAMPLES_JSONL ===")

	sum := summarize(*tag, *mode, *workload, *cacheTier, *regime, rows)
	fmt.Println("=== BEGIN_SUMMARY_JSON ===")
	sb, _ := json.MarshalIndent(sum, "", "  ")
	fmt.Println(string(sb))
	fmt.Println("=== END_SUMMARY_JSON ===")

	if sum.FailedSamples > 0 {
		os.Exit(1)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(2)
}

func (r *runner) ensureAtespace(ctx context.Context) error {
	_, err := r.ctrl.CreateAtespace(ctx, &ateapipb.CreateAtespaceRequest{
		Atespace: &ateapipb.Atespace{
			Metadata: &ateapipb.ResourceMetadata{Name: r.atespace},
		},
	})
	if err == nil || status.Code(err) == codes.AlreadyExists {
		return nil
	}
	return err
}

func (r *runner) templateForWorkload(workload string) string {
	if workload == "mem" {
		return "glutton"
	}
	return r.durdirTemplate
}

func (r *runner) createAndPopulateActor(ctx context.Context, workload string, selector map[string]string) (*actorExpect, error) {
	const maxAttempts = 8
	var lastErr error
	for attempt := range maxAttempts {
		exp, err := r.createAndPopulateActorOnce(ctx, workload, selector)
		if err == nil {
			return exp, nil
		}
		lastErr = err
		if status.Code(err) != codes.Unavailable && !strings.Contains(err.Error(), "Unavailable") {
			return nil, err
		}
		fmt.Fprintf(os.Stderr, "WARN: transient actor setup failure (attempt %d/%d): %v; retrying\n", attempt+1, maxAttempts, err)
		time.Sleep(500 * time.Millisecond)
	}
	return nil, lastErr
}

func (r *runner) createAndPopulateActorOnce(ctx context.Context, workload string, selector map[string]string) (*actorExpect, error) {
	name := "fu-" + uuid.NewString()
	actorSpec := &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: r.atespace, Name: name},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: r.templateAtespace, Name: r.templateForWorkload(workload)},
	}
	if len(selector) > 0 {
		actorSpec.WorkerSelector = &ateapipb.Selector{MatchLabels: selector}
	}
	created, err := r.ctrl.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: actorSpec})
	if err != nil {
		return nil, fmt.Errorf("CreateActor %s: %w", name, err)
	}
	exp := &actorExpect{
		name:            name,
		uid:             created.GetMetadata().GetUid(),
		sentinelDigests: make(map[int]string),
	}

	t0 := time.Now()
	var resumed *ateapipb.ResumeActorResponse
	err = boomerutil.RetryOnConflict(ctx, func() error {
		var rerr error
		resumed, rerr = r.ctrl.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: name},
		})
		return rerr
	})
	if err != nil {
		_ = r.deleteActor(ctx, name)
		return nil, fmt.Errorf("FirstResume %s: %w", name, err)
	}
	exp.firstResumeMs = msSince(t0)
	exp.sourceNode = resumed.GetActor().GetStatus().GetWorkerAssignment().GetNodeName()

	if err := r.populateWorkload(ctx, exp, workload); err != nil {
		_ = r.deleteActor(ctx, name)
		return nil, fmt.Errorf("populate %s: %w", name, err)
	}
	return exp, nil
}

func (r *runner) suspendActor(ctx context.Context, name string, syncMode bool) (float64, error) {
	mode := ateapipb.DurabilityMode_DURABILITY_MODE_ASYNCHRONOUS
	if syncMode {
		mode = ateapipb.DurabilityMode_DURABILITY_MODE_SYNCHRONOUS
	}
	t0 := time.Now()
	err := boomerutil.RetryOnConflict(ctx, func() error {
		_, serr := r.ctrl.SuspendActor(ctx, &ateapipb.SuspendActorRequest{
			Actor:   &ateapipb.ObjectRef{Atespace: r.atespace, Name: name},
			Options: &ateapipb.SuspendOptions{Mode: mode},
		})
		return serr
	})
	return msSince(t0), err
}

func (r *runner) updateActorSelector(ctx context.Context, name string, labels map[string]string) error {
	return boomerutil.RetryOnConflict(ctx, func() error {
		cur, err := r.ctrl.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: name},
		})
		if err != nil {
			return err
		}
		upd := proto.Clone(cur).(*ateapipb.Actor)
		upd.Status = nil
		upd.WorkerSelector = &ateapipb.Selector{MatchLabels: labels}
		_, err = r.ctrl.UpdateActor(ctx, &ateapipb.UpdateActorRequest{Actor: upd})
		return err
	})
}

func (r *runner) waitForDetached(ctx context.Context, name string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		cur, err := r.ctrl.GetActor(ctx, &ateapipb.GetActorRequest{
			Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: name},
		})
		if err != nil {
			return err
		}
		if cur.GetStatus().GetAssignedNode() == "" {
			return nil
		}
		time.Sleep(150 * time.Millisecond)
	}
	return fmt.Errorf("actor %s remained pinned past %v", name, timeout)
}

func (r *runner) deleteActor(ctx context.Context, name string) error {
	_, err := r.ctrl.DeleteActor(ctx, &ateapipb.DeleteActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: name},
	})
	if status.Code(err) == codes.NotFound {
		return nil
	}
	return err
}

func (r *runner) populateWorkload(ctx context.Context, exp *actorExpect, workload string) error {
	switch workload {
	case "mem":
		wReq, _ := proto.Marshal(&gluttonpb.WriteRAMRequest{
			Key:       memLoadKey,
			Size:      "512Mi",
			WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
		})
		t0 := time.Now()
		if _, err := r.postProto(ctx, exp.name, "/writeram", wReq); err != nil {
			return err
		}
		exp.fillWriteMs = msSince(t0)

		rReq, _ := proto.Marshal(&gluttonpb.ReadRAMRequest{Key: memLoadKey})
		respBytes, err := r.postProto(ctx, exp.name, "/readram", rReq)
		if err != nil {
			return err
		}
		var rResp gluttonpb.ReadRAMResponse
		if err := proto.Unmarshal(respBytes, &rResp); err != nil {
			return err
		}
		if rResp.GetSize() != 512*mibBytes {
			return fmt.Errorf("ReadRAM size=%d, want %d", rResp.GetSize(), 512*mibBytes)
		}
		exp.memChecksum = rResp.GetChecksum()
		return nil

	case "durdir":
		wReq, _ := proto.Marshal(&gluttonpb.WriteDiskRequest{
			Key:       durDirTestFile,
			Size:      512 * mibBytes,
			WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
		})
		t0 := time.Now()
		respBytes, err := r.postProto(ctx, exp.name, "/writedisk", wReq)
		if err != nil {
			return err
		}
		exp.fillWriteMs = msSince(t0)
		var wResp gluttonpb.WriteDiskResponse
		if err := proto.Unmarshal(respBytes, &wResp); err != nil {
			return err
		}
		if wResp.GetSize() != 512*mibBytes || len(wResp.GetSha256()) == 0 {
			return fmt.Errorf("WriteDisk invalid size=%d shaLen=%d", wResp.GetSize(), len(wResp.GetSha256()))
		}
		exp.singleSha256 = hex.EncodeToString(wResp.GetSha256())
		return nil

	case "smallfiles512":
		const numFiles = 2048
		const fileSize = 262144
		steps := make([]*gluttonpb.Step, numFiles)
		for i := range numFiles {
			steps[i] = &gluttonpb.Step{
				Kind: &gluttonpb.Step_Operation{
					Operation: &gluttonpb.Operation{
						Requests: []*gluttonpb.Request{{
							Kind: &gluttonpb.Request_WriteDisk{
								WriteDisk: &gluttonpb.WriteDiskRequest{
									Key:       fmt.Sprintf("%s-%04d", durDirTestFile, i),
									Size:      fileSize,
									WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
								},
							},
						}},
					},
				},
			}
		}
		reqBytes, _ := proto.Marshal(&gluttonpb.RunScriptRequest{Script: &gluttonpb.Block{Steps: steps}})
		t0 := time.Now()
		respBytes, err := r.postProto(ctx, exp.name, "/runscript", reqBytes)
		if err != nil {
			return err
		}
		exp.fillWriteMs = msSince(t0)
		var sResp gluttonpb.RunScriptResponse
		if err := proto.Unmarshal(respBytes, &sResp); err != nil {
			return err
		}
		st := sResp.GetResult().GetStats()
		if st.GetRequestsRun() != numFiles || st.GetDiskBytesWritten() != int64(numFiles*fileSize) {
			return fmt.Errorf("RunScript write mismatch: reqs=%d bytes=%d", st.GetRequestsRun(), st.GetDiskBytesWritten())
		}
		for _, idx := range []int{0, 1023, 2047} {
			d, err := r.readSingleFileDigest(ctx, exp.name, fmt.Sprintf("%s-%04d", durDirTestFile, idx), fileSize)
			if err != nil {
				return err
			}
			exp.sentinelDigests[idx] = d
		}
		return nil
	default:
		return fmt.Errorf("unknown workload %q", workload)
	}
}

func (r *runner) verifyWorkloadAfterResume(ctx context.Context, exp *actorExpect, workload string) (float64, error) {
	switch workload {
	case "mem":
		rReq, _ := proto.Marshal(&gluttonpb.ReadRAMRequest{Key: memLoadKey})
		t0 := time.Now()
		respBytes, err := r.postProto(ctx, exp.name, "/readram", rReq)
		elapsed := msSince(t0)
		if err != nil {
			return elapsed, err
		}
		var rResp gluttonpb.ReadRAMResponse
		if err := proto.Unmarshal(respBytes, &rResp); err != nil {
			return elapsed, err
		}
		if rResp.GetSize() != 512*mibBytes || rResp.GetChecksum() != exp.memChecksum {
			return elapsed, fmt.Errorf("ReadRAM mismatch: size=%d (want %d) csum=%d (want %d)", rResp.GetSize(), 512*mibBytes, rResp.GetChecksum(), exp.memChecksum)
		}
		return elapsed, nil

	case "durdir":
		rReq, _ := proto.Marshal(&gluttonpb.ReadDiskRequest{
			Key:      durDirTestFile,
			ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY,
		})
		t0 := time.Now()
		respBytes, err := r.postProto(ctx, exp.name, "/readdisk", rReq)
		elapsed := msSince(t0)
		if err != nil {
			return elapsed, err
		}
		var rResp gluttonpb.ReadDiskResponse
		if err := proto.Unmarshal(respBytes, &rResp); err != nil {
			return elapsed, err
		}
		got := hex.EncodeToString(rResp.GetSha256())
		if rResp.GetSize() != 512*mibBytes || got != exp.singleSha256 {
			return elapsed, fmt.Errorf("ReadDisk mismatch: size=%d sha=%s want=%s", rResp.GetSize(), got, exp.singleSha256)
		}
		return elapsed, nil

	case "smallfiles512":
		const numFiles = 2048
		const fileSize = 262144
		steps := make([]*gluttonpb.Step, numFiles)
		for i := range numFiles {
			steps[i] = &gluttonpb.Step{
				Kind: &gluttonpb.Step_Operation{
					Operation: &gluttonpb.Operation{
						Requests: []*gluttonpb.Request{{
							Kind: &gluttonpb.Request_ReadDisk{
								ReadDisk: &gluttonpb.ReadDiskRequest{
									Key:      fmt.Sprintf("%s-%04d", durDirTestFile, i),
									ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY,
								},
							},
						}},
					},
				},
			}
		}
		reqBytes, _ := proto.Marshal(&gluttonpb.RunScriptRequest{Script: &gluttonpb.Block{Steps: steps}})
		t0 := time.Now()
		respBytes, err := r.postProto(ctx, exp.name, "/runscript", reqBytes)
		elapsed := msSince(t0)
		if err != nil {
			return elapsed, err
		}
		var sResp gluttonpb.RunScriptResponse
		if err := proto.Unmarshal(respBytes, &sResp); err != nil {
			return elapsed, err
		}
		st := sResp.GetResult().GetStats()
		if st.GetRequestsRun() != numFiles || st.GetDiskBytesRead() != int64(numFiles*fileSize) {
			return elapsed, fmt.Errorf("RunScript read mismatch: reqs=%d bytes=%d", st.GetRequestsRun(), st.GetDiskBytesRead())
		}
		for idx, wantDigest := range exp.sentinelDigests {
			got, err := r.readSingleFileDigest(ctx, exp.name, fmt.Sprintf("%s-%04d", durDirTestFile, idx), fileSize)
			if err != nil {
				return elapsed, err
			}
			if got != wantDigest {
				return elapsed, fmt.Errorf("sentinel %d sha mismatch: got %s want %s", idx, got, wantDigest)
			}
		}
		return elapsed, nil
	default:
		return 0, fmt.Errorf("unknown workload %q", workload)
	}
}

func (r *runner) readSingleFileDigest(ctx context.Context, actorName, key string, wantSize int64) (string, error) {
	rReq, _ := proto.Marshal(&gluttonpb.ReadDiskRequest{
		Key:      key,
		ReadMode: gluttonpb.ReadMode_READ_MODE_DIGEST_ONLY,
	})
	respBytes, err := r.postProto(ctx, actorName, "/readdisk", rReq)
	if err != nil {
		return "", err
	}
	var rResp gluttonpb.ReadDiskResponse
	if err := proto.Unmarshal(respBytes, &rResp); err != nil {
		return "", err
	}
	if rResp.GetSize() != wantSize {
		return "", fmt.Errorf("file %s size=%d want=%d", key, rResp.GetSize(), wantSize)
	}
	return hex.EncodeToString(rResp.GetSha256()), nil
}

func (r *runner) postProto(ctx context.Context, actorName, route string, body []byte) ([]byte, error) {
	var lastErr error
	for attempt := range 4 {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt*150) * time.Millisecond)
		}
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, r.routerURL+route, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set(atenet.TargetActorHeader, r.atespace+"/"+actorName)
		resp, err := r.httpClient.Do(httpReq)
		if err != nil {
			lastErr = err
			continue
		}
		respBody, rerr := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if rerr != nil {
			lastErr = rerr
			continue
		}
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
			continue
		}
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
		}
		return respBody, nil
	}
	return nil, lastErr
}

func (r *runner) execOnNode(ctx context.Context, nodeName, shellCmd string) (string, error) {
	pods, err := r.k8sClient.CoreV1().Pods("kube-system").List(ctx, metav1.ListOptions{
		LabelSelector: "app=bench-hostexec",
		FieldSelector: "spec.nodeName=" + nodeName,
	})
	if err != nil {
		return "", fmt.Errorf("list bench-hostexec on %s: %w", nodeName, err)
	}
	if len(pods.Items) == 0 {
		return "", fmt.Errorf("no bench-hostexec pod found on node %s", nodeName)
	}
	podName := pods.Items[0].Name
	req := r.k8sClient.CoreV1().RESTClient().Post().
		Resource("pods").
		Name(podName).
		Namespace("kube-system").
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: "hostexec",
			Command:   []string{"nsenter", "-t", "1", "-m", "-u", "-i", "-n", "-p", "--", "bash", "-c", shellCmd},
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(r.k8sCfg, http.MethodPost, req.URL())
	if err != nil {
		return "", fmt.Errorf("spdy executor: %w", err)
	}
	var stdout, stderr bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: &stdout, Stderr: &stderr}); err != nil {
		return stdout.String(), fmt.Errorf("exec on %s (%s): %w; stderr=%s", nodeName, podName, err, strings.TrimSpace(stderr.String()))
	}
	return stdout.String(), nil
}

func (r *runner) readDiskSectors(ctx context.Context, nodeName string, diskRE *regexp.Regexp) (uint64, error) {
	out, err := r.execOnNode(ctx, nodeName, "cat /proc/diskstats")
	if err != nil {
		return 0, err
	}
	var total uint64
	for line := range strings.SplitSeq(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		dev := fields[2]
		if !diskRE.MatchString(dev) {
			continue
		}
		sectorsRead, err := strconv.ParseUint(fields[5], 10, 64)
		if err != nil {
			continue
		}
		total += sectorsRead
	}
	return total, nil
}

func (r *runner) evictNodeCaches(ctx context.Context, nodeName, cacheTier string, actorUIDs []string) error {
	var script strings.Builder
	script.WriteString("set -e\n")
	for _, uid := range actorUIDs {
		if uid != "" {
			fmt.Fprintf(&script, "rm -rf /var/lib/ate/ram-cache/%s\n", uid)
		}
	}
	if cacheTier == "cold-storage" {
		script.WriteString("sync\n")
		script.WriteString("echo 3 > /proc/sys/vm/drop_caches\n")
	}
	_, err := r.execOnNode(ctx, nodeName, script.String())
	return err
}

func (r *runner) runColdSameNode(
	ctx context.Context,
	tag, workload, cacheTier string,
	batches, batchSize, resumeConcurrency int,
	targetNode string,
	diskRE *regexp.Regexp,
) ([]sampleRow, error) {
	var allRows []sampleRow
	for b := range batches {
		fmt.Fprintf(os.Stderr, "[%s] cold-same-node batch %d/%d (%d actors, workload=%s, tier=%s)\n",
			time.Now().UTC().Format(time.RFC3339), b+1, batches, batchSize, workload, cacheTier)

		actors := make([]*actorExpect, 0, batchSize)
		for i := range batchSize {
			exp, err := r.createAndPopulateActor(ctx, workload, nil)
			if err != nil {
				for _, prev := range actors {
					_ = r.deleteActor(ctx, prev.name)
				}
				return nil, fmt.Errorf("batch %d actor %d create/populate: %w", b, i, err)
			}
			suspMs, err := r.suspendActor(ctx, exp.name, false)
			if err != nil {
				_ = r.deleteActor(ctx, exp.name)
				for _, prev := range actors {
					_ = r.deleteActor(ctx, prev.name)
				}
				return nil, fmt.Errorf("batch %d actor %d cold suspend: %w", b, i, err)
			}
			exp.coldSuspendMs = suspMs
			actors = append(actors, exp)
		}

		uids := make([]string, len(actors))
		for i, a := range actors {
			uids[i] = a.uid
		}
		if err := r.evictNodeCaches(ctx, targetNode, cacheTier, uids); err != nil {
			for _, a := range actors {
				_ = r.deleteActor(ctx, a.name)
			}
			return nil, fmt.Errorf("batch %d evict caches: %w", b, err)
		}
		time.Sleep(250 * time.Millisecond)

		sectorsBefore, err := r.readDiskSectors(ctx, targetNode, diskRE)
		if err != nil {
			for _, a := range actors {
				_ = r.deleteActor(ctx, a.name)
			}
			return nil, fmt.Errorf("batch %d diskstats before: %w", b, err)
		}

		batchRows := make([]sampleRow, batchSize)
		sem := make(chan struct{}, max(1, resumeConcurrency))
		var wg sync.WaitGroup
		for i, exp := range actors {
			wg.Add(1)
			go func(idx int, ex *actorExpect) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() { <-sem }()

				row := sampleRow{
					Tag:           tag,
					Mode:          "cold-same-node",
					Workload:      workload,
					CacheTier:     cacheTier,
					Batch:         b,
					Index:         b*batchSize + idx,
					ActorName:     ex.name,
					ActorUID:      ex.uid,
					SourceNode:    ex.sourceNode,
					FirstResumeMs: ex.firstResumeMs,
					FillWriteMs:   ex.fillWriteMs,
					ColdSuspendMs: ex.coldSuspendMs,
				}

				t0 := time.Now()
				var resumed *ateapipb.ResumeActorResponse
				rerr := boomerutil.RetryOnConflict(ctx, func() error {
					var err error
					resumed, err = r.ctrl.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
						Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: ex.name},
					})
					return err
				})
				row.ResumeMs = msSince(t0)
				if rerr != nil {
					row.Error = fmt.Sprintf("ResumeActor: %v", rerr)
					batchRows[idx] = row
					return
				}
				row.DestNode = resumed.GetActor().GetStatus().GetWorkerAssignment().GetNodeName()

				readMs, verr := r.verifyWorkloadAfterResume(ctx, ex, workload)
				row.PostResumeReadMs = readMs
				row.UsableTotalMs = row.ResumeMs + readMs
				if verr != nil {
					row.Error = fmt.Sprintf("verify: %v", verr)
				} else {
					row.Verified = true
				}
				batchRows[idx] = row
			}(i, exp)
		}
		wg.Wait()

		sectorsAfter, err := r.readDiskSectors(ctx, targetNode, diskRE)
		if err != nil {
			for _, a := range actors {
				_ = r.deleteActor(ctx, a.name)
			}
			return nil, fmt.Errorf("batch %d diskstats after: %w", b, err)
		}
		physReadMiB := float64(sectorsAfter-sectorsBefore) * 512.0 / float64(mibBytes)
		perActorMiB := physReadMiB / float64(batchSize)
		fmt.Fprintf(os.Stderr, "[%s] batch %d complete: phys_read_mib=%.1f (%.1f MiB/actor)\n",
			time.Now().UTC().Format(time.RFC3339), b+1, physReadMiB, perActorMiB)

		for i := range batchRows {
			batchRows[i].BatchPhysReadMiB = physReadMiB
			batchRows[i].ActorPhysReadMiB = perActorMiB
		}
		allRows = append(allRows, batchRows...)

		var delWg sync.WaitGroup
		for _, a := range actors {
			delWg.Add(1)
			go func(name string) {
				defer delWg.Done()
				_, _ = r.suspendActor(ctx, name, false)
				_ = r.deleteActor(ctx, name)
			}(a.name)
		}
		delWg.Wait()
		time.Sleep(3 * time.Second)
	}
	return allRows, nil
}

func (r *runner) runCrossNode(
	ctx context.Context,
	tag, workload, regime string,
	trials, concurrency int,
	node1, node2, labelKey, node1Label, node2Label string,
	diskRE *regexp.Regexp,
	dropNode2Cache bool,
) ([]sampleRow, error) {
	concurrency = max(1, concurrency)
	batches := (trials + concurrency - 1) / concurrency
	var allRows []sampleRow
	idxBase := 0

	for b := range batches {
		curCount := min(concurrency, trials-idxBase)
		fmt.Fprintf(os.Stderr, "[%s] cross-node batch %d/%d (%d actors, workload=%s, regime=%s)\n",
			time.Now().UTC().Format(time.RFC3339), b+1, batches, curCount, workload, regime)

		actors := make([]*actorExpect, 0, curCount)
		for i := range curCount {
			exp, err := r.createAndPopulateActor(ctx, workload, map[string]string{labelKey: node1Label})
			if err != nil {
				for _, prev := range actors {
					_ = r.deleteActor(ctx, prev.name)
				}
				return nil, fmt.Errorf("trial %d create on node1: %w", idxBase+i, err)
			}
			if exp.sourceNode != node1 {
				_ = r.deleteActor(ctx, exp.name)
				for _, prev := range actors {
					_ = r.deleteActor(ctx, prev.name)
				}
				return nil, fmt.Errorf("trial %d scheduled on %q, want node1 %q", idxBase+i, exp.sourceNode, node1)
			}
			suspMs, err := r.suspendActor(ctx, exp.name, false)
			if err != nil {
				_ = r.deleteActor(ctx, exp.name)
				for _, prev := range actors {
					_ = r.deleteActor(ctx, prev.name)
				}
				return nil, fmt.Errorf("trial %d suspend on node1: %w", idxBase+i, err)
			}
			exp.coldSuspendMs = suspMs
			if regime == "pre-detached" {
				if err := r.waitForDetached(ctx, exp.name, 90*time.Second); err != nil {
					_ = r.deleteActor(ctx, exp.name)
					for _, prev := range actors {
						_ = r.deleteActor(ctx, prev.name)
					}
					return nil, err
				}
			}
			if err := r.updateActorSelector(ctx, exp.name, map[string]string{labelKey: node2Label}); err != nil {
				_ = r.deleteActor(ctx, exp.name)
				for _, prev := range actors {
					_ = r.deleteActor(ctx, prev.name)
				}
				return nil, fmt.Errorf("trial %d update selector to node2: %w", idxBase+i, err)
			}
			actors = append(actors, exp)
		}

		uids := make([]string, len(actors))
		for i, a := range actors {
			uids[i] = a.uid
		}
		if dropNode2Cache {
			if err := r.evictNodeCaches(ctx, node2, "cold-storage", uids); err != nil {
				for _, a := range actors {
					_ = r.deleteActor(ctx, a.name)
				}
				return nil, fmt.Errorf("evict node2 caches: %w", err)
			}
			time.Sleep(150 * time.Millisecond)
		}

		sectorsBefore, _ := r.readDiskSectors(ctx, node2, diskRE)

		batchRows := make([]sampleRow, curCount)
		var wg sync.WaitGroup
		for i, exp := range actors {
			wg.Add(1)
			go func(slot int, ex *actorExpect) {
				defer wg.Done()
				row := sampleRow{
					Tag:           tag,
					Mode:          "cross-node",
					Workload:      workload,
					Regime:        regime,
					Batch:         b,
					Index:         idxBase + slot,
					ActorName:     ex.name,
					ActorUID:      ex.uid,
					SourceNode:    ex.sourceNode,
					FirstResumeMs: ex.firstResumeMs,
					FillWriteMs:   ex.fillWriteMs,
					ColdSuspendMs: ex.coldSuspendMs,
				}
				t0 := time.Now()
				var resumed *ateapipb.ResumeActorResponse
				rerr := boomerutil.RetryOnConflict(ctx, func() error {
					var err error
					resumed, err = r.ctrl.ResumeActor(ctx, &ateapipb.ResumeActorRequest{
						Actor: &ateapipb.ObjectRef{Atespace: r.atespace, Name: ex.name},
					})
					return err
				})
				row.ResumeMs = msSince(t0)
				if rerr != nil {
					row.Error = fmt.Sprintf("CrossNode ResumeActor: %v", rerr)
					batchRows[slot] = row
					return
				}
				row.DestNode = resumed.GetActor().GetStatus().GetWorkerAssignment().GetNodeName()
				if row.DestNode != node2 {
					row.Error = fmt.Sprintf("dest node=%q, want %q", row.DestNode, node2)
					batchRows[slot] = row
					return
				}

				readMs, verr := r.verifyWorkloadAfterResume(ctx, ex, workload)
				row.PostResumeReadMs = readMs
				row.UsableTotalMs = row.ResumeMs + readMs
				if verr != nil {
					row.Error = fmt.Sprintf("verify on node2: %v", verr)
				} else {
					row.Verified = true
				}
				batchRows[slot] = row
			}(i, exp)
		}
		wg.Wait()

		sectorsAfter, _ := r.readDiskSectors(ctx, node2, diskRE)
		physReadMiB := float64(sectorsAfter-sectorsBefore) * 512.0 / float64(mibBytes)
		perActorMiB := physReadMiB / float64(curCount)
		for i := range batchRows {
			batchRows[i].BatchPhysReadMiB = physReadMiB
			batchRows[i].ActorPhysReadMiB = perActorMiB
		}
		allRows = append(allRows, batchRows...)
		idxBase += curCount

		var delWg sync.WaitGroup
		for _, a := range actors {
			delWg.Add(1)
			go func(name string) {
				defer delWg.Done()
				_, _ = r.suspendActor(ctx, name, false)
				_ = r.deleteActor(ctx, name)
			}(a.name)
		}
		delWg.Wait()
		time.Sleep(2 * time.Second)
	}
	return allRows, nil
}

func (r *runner) runGuestWriteIO(ctx context.Context, tag, workload string, actorsCount int) ([]sampleRow, error) {
	var rows []sampleRow
	for i := range actorsCount {
		exp, err := r.createAndPopulateActor(ctx, workload, nil)
		if err != nil {
			return nil, err
		}
		overwriteMs, err := r.overwriteWorkload(ctx, exp.name, workload)
		if err != nil {
			_ = r.deleteActor(ctx, exp.name)
			return nil, err
		}
		suspMs, err := r.suspendActor(ctx, exp.name, false)
		_ = r.deleteActor(ctx, exp.name)
		if err != nil {
			return nil, err
		}
		rows = append(rows, sampleRow{
			Tag:              tag,
			Mode:             "guest-write-io",
			Workload:         workload,
			Index:            i,
			ActorName:        exp.name,
			ActorUID:         exp.uid,
			SourceNode:       exp.sourceNode,
			FirstResumeMs:    exp.firstResumeMs,
			FillWriteMs:      exp.fillWriteMs,
			PostResumeReadMs: overwriteMs,
			ColdSuspendMs:    suspMs,
			Verified:         true,
		})
	}
	return rows, nil
}

func (r *runner) overwriteWorkload(ctx context.Context, actorName, workload string) (float64, error) {
	switch workload {
	case "durdir":
		wReq, _ := proto.Marshal(&gluttonpb.WriteDiskRequest{
			Key:       durDirTestFile,
			Size:      32 * mibBytes,
			WriteMode: gluttonpb.WriteMode_WRITE_MODE_OVERWRITE_ROTATE,
		})
		t0 := time.Now()
		if _, err := r.postProto(ctx, actorName, "/writedisk", wReq); err != nil {
			return 0, err
		}
		return msSince(t0), nil
	case "smallfiles512":
		const churnFiles = 128
		const fileSize = 262144
		steps := make([]*gluttonpb.Step, churnFiles)
		for i := range churnFiles {
			steps[i] = &gluttonpb.Step{
				Kind: &gluttonpb.Step_Operation{
					Operation: &gluttonpb.Operation{
						Requests: []*gluttonpb.Request{{
							Kind: &gluttonpb.Request_WriteDisk{
								WriteDisk: &gluttonpb.WriteDiskRequest{
									Key:       fmt.Sprintf("%s-%04d", durDirTestFile, i),
									Size:      fileSize,
									WriteMode: gluttonpb.WriteMode_WRITE_MODE_TRUNCATE,
								},
							},
						}},
					},
				},
			}
		}
		reqBytes, _ := proto.Marshal(&gluttonpb.RunScriptRequest{Script: &gluttonpb.Block{Steps: steps}})
		t0 := time.Now()
		if _, err := r.postProto(ctx, actorName, "/runscript", reqBytes); err != nil {
			return 0, err
		}
		return msSince(t0), nil
	default:
		return 0, fmt.Errorf("unsupported guest-write-io workload %q", workload)
	}
}

func msSince(t0 time.Time) float64 {
	return float64(time.Since(t0).Microseconds()) / 1000.0
}

func calcDist(vals []float64) distStats {
	if len(vals) == 0 {
		return distStats{}
	}
	cp := slices.Clone(vals)
	slices.SortFunc(cp, cmp.Compare[float64])
	var sum float64
	for _, v := range cp {
		sum += v
	}
	pct := func(p float64) float64 {
		idx := int(math.Ceil(p*float64(len(cp)))) - 1
		idx = max(0, min(len(cp)-1, idx))
		return math.Round(cp[idx]*10) / 10
	}
	return distStats{
		N:    len(cp),
		Min:  math.Round(cp[0]*10) / 10,
		P50:  pct(0.50),
		P90:  pct(0.90),
		P99:  pct(0.99),
		Max:  math.Round(cp[len(cp)-1]*10) / 10,
		Mean: math.Round((sum/float64(len(cp)))*10) / 10,
	}
}

func summarize(tag, mode, workload, cacheTier, regime string, rows []sampleRow) summaryReport {
	var firstRes, fillW, coldSusp, res, postRead, usable, phys []float64
	verified := 0
	failed := 0
	for _, row := range rows {
		if !row.Verified || row.Error != "" {
			failed++
			continue
		}
		verified++
		firstRes = append(firstRes, row.FirstResumeMs)
		fillW = append(fillW, row.FillWriteMs)
		coldSusp = append(coldSusp, row.ColdSuspendMs)
		res = append(res, row.ResumeMs)
		postRead = append(postRead, row.PostResumeReadMs)
		usable = append(usable, row.UsableTotalMs)
		phys = append(phys, row.ActorPhysReadMiB)
	}
	var meanPhys float64
	if len(phys) > 0 {
		for _, v := range phys {
			meanPhys += v
		}
		meanPhys = math.Round((meanPhys/float64(len(phys)))*10) / 10
	}
	return summaryReport{
		Tag:                  tag,
		Mode:                 mode,
		Workload:             workload,
		CacheTier:            cacheTier,
		Regime:               regime,
		TotalSamples:         len(rows),
		VerifiedSamples:      verified,
		FailedSamples:        failed,
		MeanPhysReadMiBActor: meanPhys,
		FirstResume:          calcDist(firstRes),
		FillWrite:            calcDist(fillW),
		ColdSuspend:          calcDist(coldSusp),
		Resume:               calcDist(res),
		PostResumeRead:       calcDist(postRead),
		UsableTotal:          calcDist(usable),
	}
}
