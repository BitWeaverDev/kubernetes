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
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/utils/clock"
)

// idleShrinkJitterFactor randomizes the idle-shrink loop's tick interval so
// that many watchCache instances - one per resource type, including every
// CRD - don't all wake up and take their write lock in lockstep.
const idleShrinkJitterFactor = 0.25

// idleShrinker periodically gives a watchCache's event history a chance to
// shrink its ring buffer back down, even when no new events are arriving
// for that resource to drive it via watchCache.processEvent. See
// watchCacheHistory.shrinkIfIdleLocked for why that's necessary.
type idleShrinker struct {
	clock  clock.Clock
	wc     *watchCache
	period time.Duration
}

func newIdleShrinker(wc *watchCache, clock clock.Clock, period time.Duration) *idleShrinker {
	return &idleShrinker{clock: clock, wc: wc, period: period}
}

// Run blocks, ticking roughly every period (jittered) until stopCh is
// closed.
func (s *idleShrinker) Run(stopCh <-chan struct{}) {
	timer := s.clock.NewTimer(wait.Jitter(s.period, idleShrinkJitterFactor))
	defer timer.Stop()
	for {
		select {
		case <-stopCh:
			return
		case <-timer.C():
			s.wc.shrinkHistoryIfIdle(s.clock.Now())
			timer.Reset(wait.Jitter(s.period, idleShrinkJitterFactor))
		}
	}
}
