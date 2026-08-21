package proxypool

import (
	"errors"
	"fmt"
	"strings"
)

type ImportCandidate struct {
	Line     int
	Name     string
	ProxyURL string
}

type ImportFailure struct {
	Line  int    `json:"line"`
	Input string `json:"input"`
	Error string `json:"error"`
}

type ImportSkipped struct {
	Line   int
	Input  string
	Reason string
	Record *Record
}

type ImportResult struct {
	Created  []*Record
	Skipped  []ImportSkipped
	Failures []ImportFailure
}

func ParseImportText(text string) ([]ImportCandidate, []ImportFailure) {
	var candidates []ImportCandidate
	var failures []ImportFailure
	normalized := strings.ReplaceAll(strings.ReplaceAll(text, "\r\n", "\n"), "\r", "\n")
	for i, raw := range strings.Split(normalized, "\n") {
		lineNo := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name, proxyURL := splitImportLine(line)
		proxyURL = strings.TrimSpace(proxyURL)
		if proxyURL == "" {
			failures = append(failures, ImportFailure{Line: lineNo, Input: line, Error: "proxy url required"})
			continue
		}
		if _, err := NormalizeProxyURL(proxyURL); err != nil {
			failures = append(failures, ImportFailure{Line: lineNo, Input: line, Error: err.Error()})
			continue
		}
		name = strings.TrimSpace(name)
		if name == "" {
			name = DefaultName(proxyURL)
		}
		candidates = append(candidates, ImportCandidate{
			Line:     lineNo,
			Name:     name,
			ProxyURL: proxyURL,
		})
	}
	return candidates, failures
}

func splitImportLine(line string) (string, string) {
	if left, right, ok := strings.Cut(line, "----"); ok {
		return left, right
	}
	fields := strings.Fields(line)
	if len(fields) >= 2 && !strings.Contains(fields[0], "://") && strings.Contains(fields[len(fields)-1], "://") {
		return strings.Join(fields[:len(fields)-1], " "), fields[len(fields)-1]
	}
	return "", line
}

func ImportProxyRecords(repo *Repo, text string) ImportResult {
	candidates, failures := ParseImportText(text)
	created := make([]*Record, 0, len(candidates))
	skipped := make([]ImportSkipped, 0)
	seen := make(map[string]*Record)
	for _, candidate := range candidates {
		rec := &Record{
			Name:     candidate.Name,
			ProxyURL: candidate.ProxyURL,
			IsActive: true,
		}
		if err := normalizeRecord(rec); err != nil {
			failures = append(failures, ImportFailure{
				Line:  candidate.Line,
				Input: redactedImportInput(candidate.ProxyURL),
				Error: err.Error(),
			})
			continue
		}
		if previous := seen[rec.ProxyKey]; previous != nil {
			skipped = append(skipped, ImportSkipped{
				Line:   candidate.Line,
				Input:  rec.RedactedURL(),
				Reason: "duplicate",
				Record: previous,
			})
			continue
		}
		existing, err := repo.FindDuplicate(rec.ProxyURL)
		if err == nil {
			skipped = append(skipped, ImportSkipped{
				Line:   candidate.Line,
				Input:  rec.RedactedURL(),
				Reason: "duplicate",
				Record: existing,
			})
			seen[rec.ProxyKey] = existing
			continue
		}
		if err != nil && !errors.Is(err, ErrNotFound) {
			failures = append(failures, ImportFailure{
				Line:  candidate.Line,
				Input: rec.RedactedURL(),
				Error: fmt.Sprintf("duplicate check failed: %v", err),
			})
			continue
		}
		if err := repo.Create(rec); err != nil {
			if errors.Is(err, ErrDuplicate) {
				if existing, findErr := repo.FindDuplicate(rec.ProxyURL); findErr == nil {
					skipped = append(skipped, ImportSkipped{
						Line:   candidate.Line,
						Input:  rec.RedactedURL(),
						Reason: "duplicate",
						Record: existing,
					})
					seen[rec.ProxyKey] = existing
					continue
				}
			}
			failures = append(failures, ImportFailure{
				Line:  candidate.Line,
				Input: rec.RedactedURL(),
				Error: fmt.Sprintf("create failed: %v", err),
			})
			continue
		}
		seen[rec.ProxyKey] = rec
		created = append(created, rec)
	}
	return ImportResult{Created: created, Skipped: skipped, Failures: failures}
}

func redactedImportInput(raw string) string {
	if redacted := RedactProxyURL(raw); redacted != "" {
		return redacted
	}
	return strings.TrimSpace(raw)
}
