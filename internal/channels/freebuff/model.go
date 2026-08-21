package freebuff

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/marktantongco/freebuff-gateway/internal/channels"
)

const defaultModel = "minimax/minimax-m2.7"

const (
	QuotaGroupUnlimited     = "unlimited"
	QuotaGroupPremiumShared = "premium_shared"
)

type ModelProfile struct {
	Canonical  string
	Aliases    []string
	AgentID    string
	QuotaGroup string
	Enabled    bool
}

var modelCatalog = []ModelProfile{
	{
		Canonical:  "minimax/minimax-m2.7",
		Aliases:    []string{"default", "minimax-m2.7", "minmax2.7"},
		AgentID:    agentID,
		QuotaGroup: QuotaGroupUnlimited,
		Enabled:    true,
	},
	{
		Canonical:  "deepseek/deepseek-v4-flash",
		Aliases:    []string{"deepseekv4flash", "deepseek-v4-flash"},
		AgentID:    "base2-free-deepseek-flash",
		QuotaGroup: QuotaGroupUnlimited,
		Enabled:    true,
	},
	{
		Canonical:  "moonshotai/kimi-k2.6",
		Aliases:    []string{"kimik2.6", "kimi-k2.6"},
		AgentID:    "base2-free-kimi",
		QuotaGroup: QuotaGroupPremiumShared,
		Enabled:    true,
	},
	{
		Canonical:  "deepseek/deepseek-v4-pro",
		Aliases:    []string{"deepseekv4pro", "deepseek-v4-pro"},
		AgentID:    "base2-free-deepseek",
		QuotaGroup: QuotaGroupPremiumShared,
		Enabled:    true,
	},
	{
		Canonical:  "z-ai/glm-5.1",
		Aliases:    []string{"glm-5.1", "glm5.1", "zai-glm-5.1"},
		AgentID:    agentID,
		QuotaGroup: QuotaGroupPremiumShared,
		Enabled:    false,
	},
}

var (
	modelAliases  = buildModelAliases(modelCatalog)
	modelProfiles = buildModelProfiles(modelCatalog)
)

func CanonicalModel(requested string) string {
	key := modelAliasKey(requested)
	if mapped, ok := modelAliases[key]; ok {
		return mapped
	}
	return strings.TrimSpace(requested)
}

func AgentIDForModel(requested string) (string, error) {
	profile, ok := ModelProfileFor(requested)
	if !ok || !profile.Enabled {
		return "", fmt.Errorf("freebuff: unsupported model %q", requested)
	}
	return profile.AgentID, nil
}

func ModelProfileFor(requested string) (ModelProfile, bool) {
	canonical := CanonicalModel(requested)
	profile, ok := modelProfiles[canonical]
	return profile, ok
}

func quotaGroupForModel(model string) (string, bool) {
	profile, ok := ModelProfileFor(model)
	if !ok {
		return "", false
	}
	return profile.QuotaGroup, true
}

func canReclaimActiveSession(activeModel, requestedModel string) bool {
	activeModel = CanonicalModel(activeModel)
	requestedModel = CanonicalModel(requestedModel)
	if activeModel == "" || requestedModel == "" || activeModel == requestedModel {
		return false
	}
	group, ok := quotaGroupForModel(activeModel)
	return ok && group == QuotaGroupUnlimited
}

func (a *Adapter) ModelCatalog() []channels.ModelInfo {
	out := make([]channels.ModelInfo, 0, len(modelCatalog))
	for _, profile := range modelCatalog {
		aliases := append([]string(nil), profile.Aliases...)
		out = append(out, channels.ModelInfo{
			ID:         profile.Canonical,
			Aliases:    aliases,
			AgentID:    profile.AgentID,
			QuotaGroup: profile.QuotaGroup,
			Enabled:    profile.Enabled,
		})
	}
	return out
}

func buildModelAliases(catalog []ModelProfile) map[string]string {
	out := map[string]string{"": defaultModel}
	for _, profile := range catalog {
		out[modelAliasKey(profile.Canonical)] = profile.Canonical
		for _, alias := range profile.Aliases {
			out[modelAliasKey(alias)] = profile.Canonical
		}
	}
	return out
}

func buildModelProfiles(catalog []ModelProfile) map[string]ModelProfile {
	out := make(map[string]ModelProfile, len(catalog))
	for _, profile := range catalog {
		out[profile.Canonical] = profile
	}
	return out
}

func modelAliasKey(model string) string {
	return strings.ToLower(strings.TrimSpace(model))
}

func decodeChatBody(raw []byte) (map[string]any, string, bool, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var body map[string]any
	if err := dec.Decode(&body); err != nil {
		return nil, "", false, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	if body == nil {
		body = map[string]any{}
	}
	model, _ := body["model"].(string)
	return body, CanonicalModel(model), boolValue(body["stream"]), nil
}

func buildCodebuffBody(requestBody map[string]any, model, runID, instanceID string, stream bool) map[string]any {
	out := make(map[string]any, len(requestBody)+3)
	for k, v := range requestBody {
		out[k] = v
	}
	out["model"] = model
	out["stream"] = stream
	out["codebuff_metadata"] = map[string]any{
		"freebuff_instance_id": instanceID,
		"run_id":               runID,
		"client_id":            clientID(),
		"cost_mode":            "free",
	}
	out["provider"] = map[string]any{
		"data_collection": "deny",
	}
	if deepSeekModels[CanonicalModel(model)] {
		if msgs, ok := out["messages"].([]any); ok {
			injectCacheControl(msgs)
		}
	}
	return out
}

func addCodebuffStop(body map[string]any) {
	body["stop"] = appendStopSequence(body["stop"], "\"cb_easp\"")
}

func appendStopSequence(value any, stop string) any {
	switch typed := value.(type) {
	case nil:
		return []string{stop}
	case string:
		if typed == "" || typed == stop {
			return []string{stop}
		}
		return []string{typed, stop}
	case []string:
		out := append([]string(nil), typed...)
		for _, existing := range out {
			if existing == stop {
				return out
			}
		}
		return append(out, stop)
	case []any:
		out := append([]any(nil), typed...)
		for _, raw := range out {
			if stringValue(raw) == stop {
				return out
			}
		}
		return append(out, stop)
	default:
		return []string{stop}
	}
}

func boolValue(v any) bool {
	b, ok := v.(bool)
	return ok && b
}

func numberInt(n json.Number) (int, bool) {
	if n == "" {
		return 0, false
	}
	i, err := strconv.Atoi(n.String())
	if err == nil {
		return i, true
	}
	f, err := strconv.ParseFloat(n.String(), 64)
	if err != nil {
		return 0, false
	}
	return int(f), true
}

func sessionState(baseURL string, session upstreamSession) channels.State {
	state := channels.State{
		stateBaseURL: baseURL,
	}
	if session.Status != "" {
		state[stateStatus] = session.Status
	}
	if session.Message != "" {
		state[stateMessage] = session.Message
	}
	if session.InstanceID != "" {
		state[stateInstanceID] = session.InstanceID
	}
	if session.Model != "" {
		state[stateModel] = CanonicalModel(session.Model)
	}
	if session.AccessTier != "" {
		state[stateAccessTier] = session.AccessTier
	}
	if admitted := parseExpiresAtUnix(session.AdmittedAt); admitted > 0 {
		state[stateAdmittedAtUnix] = admitted
	}
	if expires := parseExpiresAtUnix(session.ExpiresAt); expires > 0 {
		state[stateExpiresAtUnix] = expires
	}
	if session.RemainingMs > 0 {
		state[stateRemainingMs] = session.RemainingMs
	}
	if session.RateLimit != nil {
		state[stateRateLimit] = *session.RateLimit
	}
	if len(session.RateLimitsByModel) > 0 {
		state[stateRateLimitsByModel] = session.RateLimitsByModel
	}
	if session.RawJSON != "" {
		state[stateRawSessionJSON] = session.RawJSON
	}
	return state
}

func runtimeState(rt runtime) channels.State {
	state := channels.State{}
	if rt.adsSessionID != "" {
		state[stateAdsSessionID] = rt.adsSessionID
	}
	if rt.device.OS != "" {
		state[stateDeviceOS] = rt.device.OS
		state[stateDeviceTZ] = rt.device.Timezone
		state[stateDeviceLocale] = rt.device.Locale
		state[stateDeviceBrowserUA] = rt.device.BrowserUA
	}
	if rt.meProfile.ID != "" {
		state[stateMeID] = rt.meProfile.ID
		state[stateMeEmail] = rt.meProfile.Email
	}
	return state
}

func mergeRuntimeState(state channels.State, rt runtime) channels.State {
	extra := runtimeState(rt)
	for k, v := range extra {
		state[k] = v
	}
	return state
}

func runIDFromOutboundRequest(req *channels.OutboundRequest) string {
	if req == nil || len(req.Body) == 0 {
		return ""
	}
	var decoded struct {
		Metadata struct {
			RunID string `json:"run_id"`
		} `json:"codebuff_metadata"`
	}
	if err := json.Unmarshal(req.Body, &decoded); err != nil {
		return ""
	}
	return decoded.Metadata.RunID
}

type deviceProfile struct {
	OS        string
	Timezone  string
	Locale    string
	BrowserUA string
}

var (
	osPool = []struct {
		Name      string
		BrowserOS string
		Weight    int
	}{
		{Name: "linux", BrowserOS: "X11; Linux x86_64", Weight: 40},
		{Name: "mac", BrowserOS: "Macintosh; Intel Mac OS X 10_15_7", Weight: 40},
		{Name: "windows", BrowserOS: "Windows NT 10.0; Win64; x64", Weight: 20},
	}

	timezonePool = []string{
		"Asia/Shanghai", "Asia/Tokyo", "Asia/Seoul", "Asia/Singapore",
		"America/Los_Angeles", "America/New_York", "America/Chicago",
		"Europe/London", "Europe/Berlin", "Europe/Paris",
	}

	localePool   = []string{"en-US", "en-US", "en-US", "en-US", "zh-CN", "zh-CN", "ja-JP"}
	chromeMajors = []int{124, 125, 126, 127, 128}
)

func generateDeviceProfile() deviceProfile {
	osIdx := weightedRandom(len(osPool), func(i int) int { return osPool[i].Weight })
	chosen := osPool[osIdx]
	chromeVer := fmt.Sprintf("%d.0.%d.%d",
		chromeMajors[randomInt(len(chromeMajors))],
		6000+randomInt(500),
		100+randomInt(200),
	)
	dp := deviceProfile{
		OS:        chosen.Name,
		Timezone:  timezonePool[randomInt(len(timezonePool))],
		Locale:    localePool[randomInt(len(localePool))],
		BrowserUA: fmt.Sprintf("Mozilla/5.0 (%s) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/%s Safari/537.36", chosen.BrowserOS, chromeVer),
	}
	return dp
}

func generateAdsSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%x", b)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func randomInt(n int) int {
	if n <= 1 {
		return 0
	}
	v, err := rand.Int(rand.Reader, big.NewInt(int64(n)))
	if err != nil {
		return 0
	}
	return int(v.Int64())
}

func weightedRandom(n int, weightFn func(i int) int) int {
	total := 0
	for i := 0; i < n; i++ {
		total += weightFn(i)
	}
	r := randomInt(total)
	cumulative := 0
	for i := 0; i < n; i++ {
		cumulative += weightFn(i)
		if r < cumulative {
			return i
		}
	}
	return n - 1
}

var deepSeekModels = map[string]bool{
	"deepseek/deepseek-v4-flash": true,
	"deepseek/deepseek-v4-pro":   true,
}

func injectCacheControl(messages []any) {
	for i := 2; i < len(messages) && i < 4; i++ {
		msg, ok := messages[i].(map[string]any)
		if !ok {
			continue
		}
		content, ok := msg["content"]
		if !ok {
			continue
		}
		blocks, ok := content.([]any)
		if !ok {
			continue
		}
		for _, raw := range blocks {
			block, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if _, exists := block["cache_control"]; !exists {
				block["cache_control"] = map[string]any{"type": "ephemeral"}
			}
		}
	}
}
