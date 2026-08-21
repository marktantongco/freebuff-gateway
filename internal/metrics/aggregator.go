package metrics

import (
	"sort"
	"sync"
	"time"
)

const maxSnapshotRows = 500

type Row struct {
	ChannelID string `json:"channel_id"`
	AccountID string `json:"account_id"`
	RPM       int    `json:"rpm"`
	TPM       int    `json:"tpm"`
	Window    string `json:"window"`
}

type Series struct {
	Window      string        `json:"window"`
	Granularity string        `json:"granularity"`
	Points      []SeriesPoint `json:"points"`
}

type SeriesPoint struct {
	Timestamp int64 `json:"timestamp"`
	Requests  int   `json:"requests"`
	Tokens    int64 `json:"tokens"`
}

type Aggregator struct {
	mu     sync.RWMutex
	now    func() time.Time
	perKey map[key]*ring
}

type key struct {
	channelID string
	accountID string
}

type ring struct {
	minute        [60]bucket
	hour          [60]bucket
	lastObsSecond int64
	lastObsMinute int64
	hasObsSecond  bool
	hasObsMinute  bool
}

type bucket struct {
	stamp  int64
	reqs   int
	tokens int64
}

func NewAggregator() *Aggregator {
	return &Aggregator{
		now:    time.Now,
		perKey: make(map[key]*ring),
	}
}

func (a *Aggregator) Observe(channelID, accountID string, tokens int) {
	if channelID == "" || accountID == "" {
		return
	}
	now := a.now().UTC()
	sec := now.Unix()
	min := sec / 60
	k := key{channelID: channelID, accountID: accountID}

	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.perKey[k]
	if r == nil {
		r = &ring{}
		a.perKey[k] = r
	}
	r.observe(sec, min, tokens)
}

func (a *Aggregator) Snapshot(window time.Duration) []Row {
	now := a.now().UTC().Unix()
	useHour := window >= time.Hour
	label := "1m"
	if useHour {
		label = "1h"
	}

	a.mu.RLock()
	defer a.mu.RUnlock()

	out := make([]Row, 0, len(a.perKey))
	for k, r := range a.perKey {
		reqs, tokens := r.snapshot(now, useHour)
		if reqs == 0 {
			continue
		}
		out = append(out, Row{
			ChannelID: k.channelID,
			AccountID: k.accountID,
			RPM:       reqs,
			TPM:       int(tokens),
			Window:    label,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RPM != out[j].RPM {
			return out[i].RPM > out[j].RPM
		}
		if out[i].TPM != out[j].TPM {
			return out[i].TPM > out[j].TPM
		}
		if out[i].ChannelID != out[j].ChannelID {
			return out[i].ChannelID < out[j].ChannelID
		}
		return out[i].AccountID < out[j].AccountID
	})
	if len(out) > maxSnapshotRows {
		out = out[:maxSnapshotRows]
	}
	return out
}

func (a *Aggregator) Series(window time.Duration) Series {
	return a.seriesAt(a.now().UTC(), window)
}

func EmptySeries(now time.Time, window time.Duration) Series {
	return buildSeries(now.UTC(), window, nil)
}

func (a *Aggregator) seriesAt(now time.Time, window time.Duration) Series {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return buildSeries(now, window, func(points []SeriesPoint, firstStamp int64, useHour bool) {
		for _, r := range a.perKey {
			r.addSeries(points, firstStamp, useHour)
		}
	})
}

func buildSeries(
	now time.Time,
	window time.Duration,
	fill func(points []SeriesPoint, firstStamp int64, useHour bool),
) Series {
	useHour := window >= time.Hour
	label := "1m"
	granularity := "second"
	currentStamp := now.Unix()
	if useHour {
		label = "1h"
		granularity = "minute"
		currentStamp = currentStamp / 60
	}
	firstStamp := currentStamp - 59
	points := make([]SeriesPoint, 60)
	for i := range points {
		stamp := firstStamp + int64(i)
		ts := stamp
		if useHour {
			ts = stamp * 60
		}
		points[i].Timestamp = ts
	}
	if fill != nil {
		fill(points, firstStamp, useHour)
	}
	return Series{
		Window:      label,
		Granularity: granularity,
		Points:      points,
	}
}

func (r *ring) observe(sec, min int64, tokens int) {
	r.advanceSecond(sec)
	r.advanceMinute(min)

	secBucket := &r.minute[sec%60]
	secBucket.stamp = sec
	secBucket.reqs++
	secBucket.tokens += int64(tokens)

	minBucket := &r.hour[min%60]
	minBucket.stamp = min
	minBucket.reqs++
	minBucket.tokens += int64(tokens)
}

func (r *ring) advanceSecond(sec int64) {
	if !r.hasObsSecond {
		r.minute[sec%60] = bucket{stamp: sec}
		r.lastObsSecond = sec
		r.hasObsSecond = true
		return
	}
	if sec <= r.lastObsSecond {
		return
	}
	gap := sec - r.lastObsSecond
	if gap > 60 {
		gap = 60
	}
	for i := int64(1); i <= gap; i++ {
		tick := r.lastObsSecond + i
		r.minute[tick%60] = bucket{stamp: tick}
	}
	r.lastObsSecond = sec
}

func (r *ring) advanceMinute(min int64) {
	if !r.hasObsMinute {
		r.hour[min%60] = bucket{stamp: min}
		r.lastObsMinute = min
		r.hasObsMinute = true
		return
	}
	if min <= r.lastObsMinute {
		return
	}
	gap := min - r.lastObsMinute
	if gap > 60 {
		gap = 60
	}
	for i := int64(1); i <= gap; i++ {
		tick := r.lastObsMinute + i
		r.hour[tick%60] = bucket{stamp: tick}
	}
	r.lastObsMinute = min
}

func (r *ring) snapshot(nowSec int64, hour bool) (int, int64) {
	var reqs int
	var tokens int64
	if hour {
		nowMin := nowSec / 60
		for _, b := range r.hour {
			if b.reqs == 0 || nowMin-b.stamp >= 60 || b.stamp > nowMin {
				continue
			}
			reqs += b.reqs
			tokens += b.tokens
		}
		return reqs, tokens
	}
	for _, b := range r.minute {
		if b.reqs == 0 || nowSec-b.stamp >= 60 || b.stamp > nowSec {
			continue
		}
		reqs += b.reqs
		tokens += b.tokens
	}
	return reqs, tokens
}

func (r *ring) addSeries(points []SeriesPoint, firstStamp int64, hour bool) {
	lastStamp := firstStamp + int64(len(points))
	if hour {
		for _, b := range r.hour {
			if b.reqs == 0 || b.stamp < firstStamp || b.stamp >= lastStamp {
				continue
			}
			idx := int(b.stamp - firstStamp)
			points[idx].Requests += b.reqs
			points[idx].Tokens += b.tokens
		}
		return
	}
	for _, b := range r.minute {
		if b.reqs == 0 || b.stamp < firstStamp || b.stamp >= lastStamp {
			continue
		}
		idx := int(b.stamp - firstStamp)
		points[idx].Requests += b.reqs
		points[idx].Tokens += b.tokens
	}
}
