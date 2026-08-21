package freebuff

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type freebuffRunSetupMode string

const (
	freebuffRunSetupModeSessionReuse        freebuffRunSetupMode = "session_reuse"
	freebuffRunSetupModeSyncParallel        freebuffRunSetupMode = "sync_parallel"
	freebuffRunSetupModeParentSyncAsyncTree freebuffRunSetupMode = "parent_sync_async_tree"
)

func defaultRunSetupMode() freebuffRunSetupMode {
	return parseRunSetupMode(os.Getenv("FREEBUFF_RUN_SETUP_MODE"))
}

func parseRunSetupMode(raw string) freebuffRunSetupMode {
	switch freebuffRunSetupMode(strings.TrimSpace(raw)) {
	case freebuffRunSetupModeSessionReuse, "":
		return freebuffRunSetupModeSessionReuse
	case freebuffRunSetupModeParentSyncAsyncTree:
		return freebuffRunSetupModeParentSyncAsyncTree
	case freebuffRunSetupModeSyncParallel:
		return freebuffRunSetupModeSyncParallel
	default:
		return freebuffRunSetupModeSessionReuse
	}
}

func (m freebuffRunSetupMode) String() string {
	if m == "" {
		return string(freebuffRunSetupModeSessionReuse)
	}
	return string(m)
}

func defaultSessionRunMaxAge() time.Duration {
	raw := strings.TrimSpace(os.Getenv("FREEBUFF_SESSION_RUN_MAX_AGE_MS"))
	if raw == "" {
		return time.Hour
	}
	ms, err := strconv.Atoi(raw)
	if err != nil || ms <= 0 {
		return time.Hour
	}
	return time.Duration(ms) * time.Millisecond
}
