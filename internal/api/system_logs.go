package api

import (
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/marktantongco/freebuff-gateway/internal/systemlogs"
)

const (
	systemLogMessageMaxBytes          = 1000
	systemLogMetadataMaxBytes         = 500
	freeBuffProtocolLoginLogComponent = "freebuff_protocol_login"
)

var systemLogSensitiveQueryRE = regexp.MustCompile(`(?i)([?&](?:auth_code|code|state|token|authToken|fingerprintHash)=)[^&\s"']+`)

func (h *AdminHandler) ListSystemLogs(w http.ResponseWriter, r *http.Request) {
	if h.systemLogs == nil {
		writeJSON(w, http.StatusOK, []systemlogs.Entry{})
		return
	}
	q := systemlogs.Query{
		Component: r.URL.Query().Get("component"),
		Level:     r.URL.Query().Get("level"),
		JobID:     r.URL.Query().Get("job_id"),
	}
	if limit := r.URL.Query().Get("limit"); limit != "" {
		if n, err := strconv.Atoi(limit); err == nil {
			q.Limit = n
		}
	}
	entries, err := h.systemLogs.List(q)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if entries == nil {
		entries = []systemlogs.Entry{}
	}
	writeJSON(w, http.StatusOK, entries)
}

func (h *AdminHandler) appendSystemLog(entry systemlogs.Entry) {
	if h.systemLogs == nil {
		return
	}
	entry.Message = truncateSystemLogString(redactSystemLogString(entry.Message), systemLogMessageMaxBytes)
	entry.Metadata = sanitizeSystemLogMetadata(entry.Metadata)
	if err := h.systemLogs.Append(entry); err != nil {
		log.Printf("systemlogs: append failed component=%s event=%s: %v", entry.Component, entry.Event, err)
	}
}

func (h *AdminHandler) appendFreeBuffProtocolLoginLog(jobID, level, event, message string, metadata map[string]any) {
	h.appendSystemLog(systemlogs.Entry{
		Level:     level,
		Component: freeBuffProtocolLoginLogComponent,
		Event:     event,
		Message:   message,
		JobID:     strings.TrimSpace(jobID),
		Metadata:  metadata,
	})
}

func sanitizeSystemLogMetadata(in map[string]any) map[string]any {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]any, len(in))
	for key, value := range in {
		key = strings.TrimSpace(key)
		if key == "" {
			continue
		}
		out[key] = sanitizeSystemLogValue(value)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func sanitizeSystemLogValue(value any) any {
	switch v := value.(type) {
	case string:
		return truncateSystemLogString(redactSystemLogString(v), systemLogMetadataMaxBytes)
	case []string:
		out := make([]string, 0, len(v))
		for i, item := range v {
			if i >= 10 {
				break
			}
			out = append(out, truncateSystemLogString(item, systemLogMetadataMaxBytes))
		}
		return out
	case []any:
		out := make([]any, 0, len(v))
		for i, item := range v {
			if i >= 10 {
				break
			}
			out = append(out, sanitizeSystemLogValue(item))
		}
		return out
	case map[string]any:
		return sanitizeSystemLogMetadata(v)
	default:
		return value
	}
}

func truncateSystemLogString(value string, maxBytes int) string {
	value = strings.TrimSpace(value)
	if maxBytes <= 0 || len(value) <= maxBytes {
		return value
	}
	return value[:maxBytes]
}

func redactSystemLogString(value string) string {
	return systemLogSensitiveQueryRE.ReplaceAllString(value, "$1[redacted]")
}
