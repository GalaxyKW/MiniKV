package main

import (
	"math/bits"
	"time"
)

// arrivalOffset is exact even when rate does not divide one second. Config
// validation guarantees that the whole schedule fits in time.Duration.
func arrivalOffset(index, rate int) time.Duration {
	return time.Duration(index/rate)*time.Second +
		time.Duration(int64(index%rate)*int64(time.Second)/int64(rate))
}

// latestArrival inverts floor(index*1e9/rate), including its nanosecond rounding.
// Multiplying elapsed by rate alone is wrong at boundaries such as rate=3.
func latestArrival(elapsed time.Duration, rate int) int {
	hi, lo := bits.Mul64(uint64(elapsed)+1, uint64(rate))
	lo, borrow := bits.Sub64(lo, 1, 0)
	hi -= borrow
	index, _ := bits.Div64(hi, lo, uint64(time.Second))
	return int(index)
}

type arrivalClock interface {
	Now() time.Time
	WaitUntil(time.Time)
}

type wallArrivalClock struct{ timer *time.Timer }

func newArrivalClock() *wallArrivalClock {
	timer := time.NewTimer(time.Hour)
	timer.Stop()
	return &wallArrivalClock{timer: timer}
}

func (*wallArrivalClock) Now() time.Time { return time.Now() }

func (clock *wallArrivalClock) WaitUntil(deadline time.Time) {
	for remaining := time.Until(deadline); remaining > 0; remaining = time.Until(deadline) {
		clock.timer.Reset(remaining)
		<-clock.timer.C
	}
}

type arrivalCounts struct{ started, busy, late int }

// A slot expires at the next slot's deadline. Skip expired slots in a batch:
// replaying them would turn a delayed scheduler into a catch-up burst. The
// callback reserves one worker slot without waiting, or rejects the arrival.
func scheduleArrivals(requests, rate int, start time.Time, clock arrivalClock,
	admit func(index int, scheduledAt time.Time) bool) arrivalCounts {
	var counts arrivalCounts
	window := arrivalOffset(requests, rate)
	for next := 0; next < requests; {
		clock.WaitUntil(start.Add(arrivalOffset(next, rate)))
		elapsed := clock.Now().Sub(start)
		if elapsed >= window {
			counts.late += requests - next
			break
		}
		latest := latestArrival(elapsed, rate)
		if latest > next {
			counts.late += latest - next
			next = latest
		}
		if admit(next, start.Add(arrivalOffset(next, rate))) {
			counts.started++
		} else {
			counts.busy++
		}
		next++
	}
	// Include the whole offered-load window, even if the final request finishes
	// early. In-flight work is drained by the caller after this returns.
	clock.WaitUntil(start.Add(window))
	return counts
}
