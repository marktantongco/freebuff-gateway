package metrics

import (
	"fmt"
	"testing"
	"time"
)

func TestSnapshotEmpty(t *testing.T) {
	a := NewAggregator()
	rows := a.Snapshot(time.Minute)
	if len(rows) != 0 {
		t.Fatalf("expected empty snapshot, got %d rows", len(rows))
	}
}

func TestObserveSingleRow(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Observe("demo", "acc", 11)

	rows := a.Snapshot(time.Minute)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	row := rows[0]
	if row.ChannelID != "demo" || row.AccountID != "acc" || row.RPM != 1 || row.TPM != 11 || row.Window != "1m" {
		t.Fatalf("unexpected row: %+v", row)
	}
}

func TestSecondBucketRolloverWithinMinute(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Observe("demo", "acc", 5)
	now = now.Add(2 * time.Second)
	a.Observe("demo", "acc", 7)

	rows := a.Snapshot(time.Minute)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(rows))
	}
	if rows[0].RPM != 2 || rows[0].TPM != 12 {
		t.Fatalf("expected rpm=2 tpm=12, got %+v", rows[0])
	}
}

func TestMinuteBucketRolloverWithinHour(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Observe("demo", "acc", 5)
	now = now.Add(2 * time.Minute)
	a.Observe("demo", "acc", 7)

	minRows := a.Snapshot(time.Minute)
	if len(minRows) != 1 || minRows[0].RPM != 1 || minRows[0].TPM != 7 {
		t.Fatalf("expected only latest minute in 1m snapshot, got %+v", minRows)
	}
	hourRows := a.Snapshot(time.Hour)
	if len(hourRows) != 1 || hourRows[0].RPM != 2 || hourRows[0].TPM != 12 || hourRows[0].Window != "1h" {
		t.Fatalf("expected both requests in 1h snapshot, got %+v", hourRows)
	}
}

func TestSnapshotSortedAndCapped(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := NewAggregator()
	a.now = func() time.Time { return now }

	for i := 0; i < 510; i++ {
		accountID := fmt.Sprintf("acc-%03d", i)
		a.Observe("demo", accountID, i)
		if i == 509 {
			a.Observe("demo", accountID, i)
		}
	}

	rows := a.Snapshot(time.Minute)
	if len(rows) != maxSnapshotRows {
		t.Fatalf("expected cap %d, got %d", maxSnapshotRows, len(rows))
	}
	if rows[0].RPM != 2 {
		t.Fatalf("expected highest-rpm row first, got %+v", rows[0])
	}
}

func TestSeriesMinuteBuckets(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Observe("demo", "acc-a", 5)
	now = now.Add(2 * time.Second)
	a.Observe("demo", "acc-a", 7)
	a.Observe("demo", "acc-b", 11)

	series := a.Series(time.Minute)
	if series.Window != "1m" || series.Granularity != "second" {
		t.Fatalf("unexpected series metadata: %+v", series)
	}
	if len(series.Points) != 60 {
		t.Fatalf("expected 60 points, got %d", len(series.Points))
	}
	first := series.Points[57]
	if first.Timestamp != now.Add(-2*time.Second).Unix() || first.Requests != 1 || first.Tokens != 5 {
		t.Fatalf("unexpected first observation bucket: %+v", first)
	}
	last := series.Points[59]
	if last.Timestamp != now.Unix() || last.Requests != 2 || last.Tokens != 18 {
		t.Fatalf("unexpected latest bucket: %+v", last)
	}
}

func TestSeriesHourBuckets(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	a := NewAggregator()
	a.now = func() time.Time { return now }

	a.Observe("demo", "acc", 5)
	now = now.Add(2 * time.Minute)
	a.Observe("demo", "acc", 7)

	series := a.Series(time.Hour)
	if series.Window != "1h" || series.Granularity != "minute" {
		t.Fatalf("unexpected series metadata: %+v", series)
	}
	if len(series.Points) != 60 {
		t.Fatalf("expected 60 points, got %d", len(series.Points))
	}
	first := series.Points[57]
	if first.Timestamp != now.Add(-2*time.Minute).Truncate(time.Minute).Unix() || first.Requests != 1 || first.Tokens != 5 {
		t.Fatalf("unexpected first minute bucket: %+v", first)
	}
	last := series.Points[59]
	if last.Timestamp != now.Truncate(time.Minute).Unix() || last.Requests != 1 || last.Tokens != 7 {
		t.Fatalf("unexpected latest minute bucket: %+v", last)
	}
}

func TestEmptySeries(t *testing.T) {
	now := time.Unix(1_700_000_000, 0).UTC()
	series := EmptySeries(now, time.Minute)
	if series.Window != "1m" || len(series.Points) != 60 {
		t.Fatalf("unexpected empty series: %+v", series)
	}
	for _, p := range series.Points {
		if p.Requests != 0 || p.Tokens != 0 {
			t.Fatalf("expected zero point, got %+v", p)
		}
	}
}
