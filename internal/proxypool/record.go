package proxypool

const (
	HealthUnknown   = "unknown"
	HealthHealthy   = "healthy"
	HealthUnhealthy = "unhealthy"
)

type Record struct {
	ID            string
	Name          string
	ProxyURL      string
	ProxyKey      string
	Scheme        string
	Host          string
	Port          int
	Username      string
	IsActive      bool
	Notes         string
	HealthStatus  string
	LatencyMS     int64
	LastCheckedAt int64
	LastError     string
	FailureCount  int
	ExitIP        string
	Country       string
	Region        string
	City          string
	CreatedAt     int64
	UpdatedAt     int64
}

type ProbeMetadata struct {
	ExitIP  string
	Country string
	Region  string
	City    string
}

func (r *Record) RedactedURL() string {
	if r == nil {
		return ""
	}
	return RedactProxyURL(r.ProxyURL)
}
