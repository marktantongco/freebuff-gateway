package quota

import "time"

const (
	PeriodDay   = "day"
	PeriodWeek  = "week"
	PeriodMonth = "month"
)

func IsPeriod(period string) bool {
	switch period {
	case "", PeriodDay, PeriodWeek, PeriodMonth:
		return true
	default:
		return false
	}
}

func BucketStart(now time.Time, period string) int64 {
	u := now.UTC()
	switch period {
	case PeriodDay:
		return time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).Unix()
	case PeriodWeek:
		weekday := int(u.Weekday())
		if weekday == 0 {
			weekday = 7
		}
		monday := time.Date(u.Year(), u.Month(), u.Day(), 0, 0, 0, 0, time.UTC).
			AddDate(0, 0, -(weekday - 1))
		return monday.Unix()
	case PeriodMonth:
		return time.Date(u.Year(), u.Month(), 1, 0, 0, 0, 0, time.UTC).Unix()
	default:
		return u.Unix()
	}
}

func PeriodEnd(start int64, period string, now time.Time) time.Time {
	if start <= 0 {
		return now
	}
	s := time.Unix(start, 0).UTC()
	switch period {
	case PeriodDay:
		return s.Add(24 * time.Hour)
	case PeriodWeek:
		return s.Add(7 * 24 * time.Hour)
	case PeriodMonth:
		return s.AddDate(0, 1, 0)
	default:
		return now
	}
}

func NeedsRollover(start int64, period string, now time.Time) bool {
	return start == 0 || !now.Before(PeriodEnd(start, period, now))
}
