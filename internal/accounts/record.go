package accounts

import "github.com/marktantongco/freebuff-gateway/internal/channels"

type Record struct {
	ID               string         `json:"id"`
	ChannelID        string         `json:"channel_id"`
	Name             string         `json:"name"`
	Credential       string         `json:"-"`
	Priority         int            `json:"priority"`
	RPMLimit         int            `json:"rpm_limit"`
	QuotaTotal       int64          `json:"quota_total"`
	QuotaPeriod      string         `json:"quota_period"`
	QuotaUsed        int64          `json:"quota_used"`
	QuotaPeriodStart int64          `json:"quota_period_start"`
	IsActive         bool           `json:"is_active"`
	Metadata         map[string]any `json:"metadata,omitempty"`
	CreatedAt        int64          `json:"created_at"`
	UpdatedAt        int64          `json:"updated_at"`
}

func (r *Record) ToChannel() channels.Account {
	return channels.Account{
		ID:         r.ID,
		ChannelID:  r.ChannelID,
		Name:       r.Name,
		Credential: r.Credential,
		Metadata:   r.Metadata,
	}
}
