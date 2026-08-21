package quota

import (
	"testing"
	"time"
)

func TestBucketStartUsesUTCBoundaries(t *testing.T) {
	loc := time.FixedZone("offset", 8*60*60)
	now := time.Date(2026, 5, 17, 1, 30, 0, 0, loc)

	tests := []struct {
		name   string
		period string
		want   time.Time
	}{
		{name: "day", period: PeriodDay, want: time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC)},
		{name: "week", period: PeriodWeek, want: time.Date(2026, 5, 11, 0, 0, 0, 0, time.UTC)},
		{name: "month", period: PeriodMonth, want: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := time.Unix(BucketStart(now, tt.period), 0).UTC()
			if !got.Equal(tt.want) {
				t.Fatalf("bucket start = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestNeedsRollover(t *testing.T) {
	start := time.Date(2026, 5, 16, 0, 0, 0, 0, time.UTC).Unix()
	beforeEnd := time.Date(2026, 5, 16, 23, 59, 0, 0, time.UTC)
	atEnd := time.Date(2026, 5, 17, 0, 0, 0, 0, time.UTC)

	if NeedsRollover(start, PeriodDay, beforeEnd) {
		t.Fatalf("did not expect rollover before period end")
	}
	if !NeedsRollover(start, PeriodDay, atEnd) {
		t.Fatalf("expected rollover at period end")
	}
	if !NeedsRollover(0, PeriodDay, beforeEnd) {
		t.Fatalf("expected rollover for empty start")
	}
}
