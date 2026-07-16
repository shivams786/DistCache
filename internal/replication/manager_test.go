package replication

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/codex/distcache/internal/transport"
)

type fakeClient struct {
	replicateFailures int
	deleteFailures    int
	replicateCalls    int
	deleteCalls       int
}

func (f *fakeClient) Replicate(context.Context, string, *transport.ReplicateRequest) (*transport.ReplicateResponse, error) {
	f.replicateCalls++
	if f.replicateCalls <= f.replicateFailures {
		return nil, errors.New("replicate failed")
	}
	return &transport.ReplicateResponse{Stored: true}, nil
}

func (f *fakeClient) Delete(context.Context, string, *transport.DeleteRequest) (*transport.DeleteResponse, error) {
	f.deleteCalls++
	if f.deleteCalls <= f.deleteFailures {
		return nil, errors.New("delete failed")
	}
	return &transport.DeleteResponse{Deleted: true}, nil
}

func TestRetryDelaySchedule(t *testing.T) {
	if retryDelay(0) != 100*time.Millisecond {
		t.Fatalf("retry 0 delay = %s", retryDelay(0))
	}
	if retryDelay(1) != 200*time.Millisecond {
		t.Fatalf("retry 1 delay = %s", retryDelay(1))
	}
	if retryDelay(2) != 400*time.Millisecond {
		t.Fatalf("retry 2 delay = %s", retryDelay(2))
	}
}

func TestReplicationRetriesBeforeSuccess(t *testing.T) {
	client := &fakeClient{replicateFailures: 2}
	manager := NewWithRetries(client, 1, 1, time.Second, 3, nil, nil, nil)
	manager.process(context.Background(), Task{
		Operation: OperationSet,
		Key:       "key",
		Value:     []byte("value"),
		Target:    "node-2",
		Source:    "node-1",
	})
	if client.replicateCalls != 3 {
		t.Fatalf("expected 3 replicate calls, got %d", client.replicateCalls)
	}
}

func TestReplicationStopsAfterRetryBudget(t *testing.T) {
	client := &fakeClient{replicateFailures: 10}
	manager := NewWithRetries(client, 1, 1, time.Millisecond, 1, nil, nil, nil)
	manager.process(context.Background(), Task{
		Operation: OperationSet,
		Key:       "key",
		Value:     []byte("value"),
		Target:    "node-2",
		Source:    "node-1",
	})
	if client.replicateCalls != 2 {
		t.Fatalf("expected initial attempt plus one retry, got %d", client.replicateCalls)
	}
}

func TestDeleteReplicationUsesDeleteRPC(t *testing.T) {
	client := &fakeClient{}
	manager := NewWithRetries(client, 1, 1, time.Second, 0, nil, nil, nil)
	manager.process(context.Background(), Task{
		Operation: OperationDelete,
		Key:       "key",
		Target:    "node-2",
		Source:    "node-1",
	})
	if client.deleteCalls != 1 || client.replicateCalls != 0 {
		t.Fatalf("delete calls=%d replicate calls=%d", client.deleteCalls, client.replicateCalls)
	}
}
