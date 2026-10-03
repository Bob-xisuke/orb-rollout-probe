package store

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
)

func sampleRollout() *Rollout {
	return &Rollout{
		Namespace:           "payments",
		Workload:            "gateway",
		TargetVersion:       "v2.3.0",
		BatchSize:           4,
		MaxUnavailableRatio: 0.25,
		CompletedBatches:    2,
		Probe: Probe{
			Type:             "httpGet",
			Path:             "/ready",
			Port:             8080,
			TimeoutSeconds:   3,
			SuccessThreshold: 1,
			FailureThreshold: 3,
		},
		ProbeResults: []ProbeResult{
			{Batch: 0, Ready: true, FailedPods: 0},
			{Batch: 1, Ready: false, FailedPods: 2, Reason: "timeout"},
		},
		Rollback: false,
	}
}

func TestCreateRolloutFirstSubmissionReturnsCreatedWithID(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	rollout := sampleRollout()
	created, err := st.CreateRollout(context.Background(), rollout)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !created {
		t.Fatal("first submission should be created")
	}
	if rollout.ID == "" {
		t.Fatal("created rollout must carry a stable id")
	}

	stored, err := st.GetRollout(context.Background(), "payments", "gateway", "v2.3.0")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.ID != rollout.ID {
		t.Fatalf("id = %q, want %q", stored.ID, rollout.ID)
	}
	if stored.CompletedBatches != 2 || stored.BatchSize != 4 || stored.MaxUnavailableRatio != 0.25 {
		t.Fatalf("unexpected stored fields: %+v", stored)
	}
	if len(stored.ProbeResults) != 2 || stored.ProbeResults[1].Reason != "timeout" {
		t.Fatalf("unexpected probe results: %+v", stored.ProbeResults)
	}
}

func TestCreateRolloutIdenticalRepeatIsIdempotent(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	first := sampleRollout()
	created, err := st.CreateRollout(context.Background(), first)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if !created {
		t.Fatal("first submission should be created")
	}

	repeat := sampleRollout()
	created, err = st.CreateRollout(context.Background(), repeat)
	if err != nil {
		t.Fatalf("repeat create: %v", err)
	}
	if created {
		t.Fatal("identical repeat must not create a second row")
	}
	if repeat.ID != first.ID {
		t.Fatalf("repeat id = %q, want original %q", repeat.ID, first.ID)
	}
}

func TestCreateRolloutDifferentContentConflicts(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	first := sampleRollout()
	if _, err := st.CreateRollout(context.Background(), first); err != nil {
		t.Fatalf("create: %v", err)
	}

	conflicting := sampleRollout()
	conflicting.BatchSize = 8
	if _, err := st.CreateRollout(context.Background(), conflicting); !errors.Is(err, ErrRolloutConflict) {
		t.Fatalf("err = %v, want ErrRolloutConflict", err)
	}

	stored, err := st.GetRollout(context.Background(), "payments", "gateway", "v2.3.0")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if stored.BatchSize != 4 {
		t.Fatalf("original row was mutated: batchSize = %d", stored.BatchSize)
	}
	if stored.ID != first.ID {
		t.Fatal("original record must be preserved")
	}
}

func TestGetRolloutUnknownTripleReturnsNotFound(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	if _, err := st.GetRollout(context.Background(), "missing", "workload", "v1"); !errors.Is(err, ErrRolloutNotFound) {
		t.Fatalf("err = %v, want ErrRolloutNotFound", err)
	}
}

func TestCreateRolloutConcurrentIdenticalSubmissionsKeepOneRow(t *testing.T) {
	st, err := Open(filepath.Join(t.TempDir(), "store.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	const goroutines = 24
	var wg sync.WaitGroup
	results := make(chan bool, goroutines)
	errs := make(chan error, goroutines)
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			rollout := sampleRollout()
			created, err := st.CreateRollout(context.Background(), rollout)
			errs <- err
			results <- created
		}()
	}
	wg.Wait()
	close(results)
	close(errs)

	createdCount := 0
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent create: %v", err)
		}
	}
	for created := range results {
		if created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created rows = %d, want exactly 1", createdCount)
	}

	var total int
	if err := st.db.QueryRowContext(context.Background(), `SELECT count(*) FROM rollouts`).Scan(&total); err != nil {
		t.Fatalf("count: %v", err)
	}
	if total != 1 {
		t.Fatalf("row count = %d, want 1", total)
	}
}
