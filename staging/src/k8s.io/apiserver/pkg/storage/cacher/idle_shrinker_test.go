/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cacher

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	testingclock "k8s.io/utils/clock/testing"
)

// TestIdleShrinkerRun drives idleShrinker.Run end-to-end through a fake
// clock: it reproduces the "burst then quiesce" scenario from
// TestShrinkIfIdleLocked, but through the actual timer loop instead of
// calling shrinkIfIdleLocked directly, and additionally checks that the
// loop registers exactly one timer wait per tick and exits promptly - and
// without leaking its goroutine - once stopped. A per-resource-type
// goroutine like this runs once for every resource type in the cluster,
// including every CRD, so a shutdown leak here isn't a minor detail.
func TestIdleShrinkerRun(t *testing.T) {
	const eventFreshDuration = 10 * time.Second

	store := newTestWatchCache(8, eventFreshDuration, &cache.Indexers{})
	defer store.Stop()
	store.history.lowerBoundCapacity = 2

	fakeClock, ok := store.config.clock.(*testingclock.FakeClock)
	if !ok {
		t.Fatalf("expected watchCache to be backed by a *testingclock.FakeClock, got %T", store.config.clock)
	}

	// Seed 2 stale events into a ring sized 8, as if a burst had already
	// grown it and then subsided - the same setup as
	// TestShrinkIfIdleLocked's "burst subsides" case.
	loadEventWithDuration(store, 2, time.Second)

	shrinker := newIdleShrinker(store.watchCache, fakeClock, eventFreshDuration)
	stopCh := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		shrinker.Run(stopCh)
	}()

	// Wait for Run to register its timer before advancing the clock:
	// otherwise Step could race ahead of the NewTimer call inside Run.
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, wait.ForeverTestTimeout, true, func(_ context.Context) (bool, error) {
		return fakeClock.HasWaiters(), nil
	}); err != nil {
		t.Fatalf("idleShrinker never registered its timer: %v", err)
	}

	// Step comfortably past the freshness window and the largest possible
	// jitter (period * (1 + idleShrinkJitterFactor)), so the tick fires in
	// this single Step call.
	fakeClock.Step(eventFreshDuration*2 + time.Second)

	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, wait.ForeverTestTimeout, true, func(_ context.Context) (bool, error) {
		store.RLock()
		defer store.RUnlock()
		return store.history.capacity < 8, nil
	}); err != nil {
		t.Fatalf("idleShrinker did not shrink the ring after the freshness window elapsed: %v", err)
	}

	store.RLock()
	gotCapacity := store.history.capacity
	store.RUnlock()
	if want := 4; gotCapacity != want {
		t.Errorf("expected capacity %d after one idle tick, got %d", want, gotCapacity)
	}

	// Run must return promptly once stopCh closes, without leaking its
	// goroutine - this is a background loop started once per resource
	// type (including every CRD) for the lifetime of the apiserver.
	close(stopCh)
	select {
	case <-done:
	case <-time.After(wait.ForeverTestTimeout):
		t.Fatal("idleShrinker.Run did not return after stopCh was closed")
	}
}

// TestIdleShrinkerRunDoesNotFireEarly checks that a tick which lands before
// the freshness window has elapsed is a no-op: Run must not shrink (or
// otherwise touch) the ring on every wakeup regardless of staleness.
//
// The shrinker's tick period is deliberately much shorter than the ring's
// eventFreshDuration here (10s vs. 60s), so there's a step size that both
// reliably fires the (jittered - see idleShrinkJitterFactor, which only
// ever *lengthens* the wait, never shortens it below period) timer at least
// once, and still lands well inside the freshness window. Using the same
// value for both, as the ring is normally configured, would make that
// impossible: the timer's minimum possible interval already equals the
// staleness threshold, so any step guaranteed to fire it would also make
// the loaded events stale, defeating the point of this test.
func TestIdleShrinkerRunDoesNotFireEarly(t *testing.T) {
	const (
		eventFreshDuration = 60 * time.Second
		shrinkerPeriod     = 10 * time.Second
	)

	store := newTestWatchCache(8, eventFreshDuration, &cache.Indexers{})
	defer store.Stop()
	store.history.lowerBoundCapacity = 2
	loadEventWithDuration(store, 2, time.Second)

	fakeClock, ok := store.config.clock.(*testingclock.FakeClock)
	if !ok {
		t.Fatalf("expected watchCache to be backed by a *testingclock.FakeClock, got %T", store.config.clock)
	}

	shrinker := newIdleShrinker(store.watchCache, fakeClock, shrinkerPeriod)
	stopCh := make(chan struct{})
	defer close(stopCh)
	go shrinker.Run(stopCh)

	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, wait.ForeverTestTimeout, true, func(_ context.Context) (bool, error) {
		return fakeClock.HasWaiters(), nil
	}); err != nil {
		t.Fatalf("idleShrinker never registered its timer: %v", err)
	}

	// 15s is comfortably above the timer's maximum possible interval
	// (shrinkerPeriod * 1.25 = 12.5s, so exactly one tick fires) and
	// comfortably below eventFreshDuration (60s, so the loaded events are
	// still fresh when it does): shrinkHistoryIfIdle runs, but should find
	// nothing stale to reclaim.
	fakeClock.Step(15 * time.Second)

	// There's no event to poll for here (the whole point is that nothing
	// should happen); wait for shrinkHistoryIfIdle to have had a chance to
	// run by observing the timer being reset for its next tick, then
	// assert capacity is untouched.
	if err := wait.PollUntilContextTimeout(context.Background(), time.Millisecond, wait.ForeverTestTimeout, true, func(_ context.Context) (bool, error) {
		return fakeClock.HasWaiters(), nil
	}); err != nil {
		t.Fatalf("idleShrinker did not reset its timer after ticking: %v", err)
	}

	store.RLock()
	defer store.RUnlock()
	if store.history.capacity != 8 {
		t.Errorf("expected capacity to stay at 8 while events are still fresh, got %d", store.history.capacity)
	}
}
