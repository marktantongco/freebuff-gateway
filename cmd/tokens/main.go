// Command tokens provides CLI management for secure token discovery and decryption.
//
// Usage:
//
//	tokens discover          # Scan and list all discovered tokens
//	tokens status            # Show provider status
//	tokens validate          # Validate all tokens
//	tokens export            # Export tokens as environment variables
//	tokens refresh           # Refresh cached tokens
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/marktantongco/freebuff-gateway/internal/credential"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	config := credential.DefaultDiscoveryConfig()
	discovery := credential.NewTokenDiscovery(config)

	managerConfig := credential.DefaultManagerConfig()
	manager := credential.NewCredentialManager(discovery, managerConfig)

	registry := credential.NewProviderRegistry("data")
	registry.LoadProviders()

	switch os.Args[1] {
	case "discover":
		cmdDiscover(discovery)
	case "status":
		cmdStatus(discovery, manager, registry)
	case "validate":
		cmdValidate(manager)
	case "export":
		cmdExport(manager)
	case "refresh":
		cmdRefresh(manager)
	case "summary":
		cmdSummary(manager)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Freebuff Gateway — Token Management")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  tokens discover          Scan and list all discovered tokens")
	fmt.Println("  tokens status            Show provider status")
	fmt.Println("  tokens validate          Validate all tokens")
	fmt.Println("  tokens export            Export tokens as environment variables")
	fmt.Println("  tokens refresh           Refresh cached tokens")
	fmt.Println("  tokens summary           Show token summary")
}

func cmdDiscover(discovery *credential.TokenDiscovery) {
	fmt.Println("🔍 Scanning secure-tokens directory...")
	fmt.Println()

	if err := discovery.Scan(); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Scan failed: %v\n", err)
		os.Exit(1)
	}

	tokens := discovery.GetTokens()
	if len(tokens) == 0 {
		fmt.Println("No tokens found")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "ID\tPROVIDER\tFINGERPRINT\n")
	fmt.Fprintf(w, "---\t--------\t------------\n")

	for _, token := range discovery.SortTokensByProvider() {
		fmt.Fprintf(w, "%s\t%s\t%s\n", token.ID, token.Provider, token.Fingerprint)
	}

	w.Flush()
	fmt.Printf("\nTotal: %d tokens discovered\n", len(tokens))
}

func cmdStatus(discovery *credential.TokenDiscovery, manager *credential.CredentialManager, registry *credential.ProviderRegistry) {
	fmt.Println("📊 Token & Provider Status")
	fmt.Println()

	// Scan tokens
	if err := discovery.Scan(); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Scan failed: %v\n", err)
		os.Exit(1)
	}

	// Load provider keys
	allTokens := discovery.GetTokens()
	for _, token := range allTokens {
		decrypted, err := manager.GetToken(token.ID)
		if err == nil {
			keys := registry.GetAPIKeys(token.Provider)
			keys = append(keys, decrypted)
			registry.SetAPIKeys(token.Provider, keys)
		}
	}

	// Print provider status
	statuses := registry.GetStatus()
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "PROVIDER\tNAME\tENABLED\tVALID KEYS\tSTATUS\n")
	fmt.Fprintf(w, "--------\t----\t-------\t----------\t------\n")

	for _, status := range statuses {
		enabled := "✓"
		if !status.Enabled {
			enabled = "✗"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%s\n",
			status.Provider, status.Name, enabled, status.ValidKeys, status.TotalKeys, status.Status)
	}

	w.Flush()

	// Print cache stats
	stats := manager.GetCacheStats()
	fmt.Printf("\nCache: %d cached (%d valid, %d expired)\n",
		stats["total_cached"], stats["valid_cached"], stats["expired_cached"])
}

func cmdValidate(manager *credential.CredentialManager) {
	fmt.Println("✅ Validating all tokens...")
	fmt.Println()

	summary, err := manager.GetSummary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Discovered: %d tokens\n", summary.TotalDiscovered)
	fmt.Printf("Decrypted:  %d tokens\n", summary.TotalDecrypted)
	fmt.Printf("Valid:      %d tokens\n", summary.TotalValid)
	fmt.Println()

	if summary.TotalValid > 0 {
		fmt.Println("Valid tokens by provider:")
		for provider, count := range summary.ByProvider {
			fmt.Printf("  %s: %d\n", provider, count)
		}
	}
}

func cmdExport(manager *credential.CredentialManager) {
	fmt.Println("📦 Exporting tokens as environment variables...")
	fmt.Println()

	creds, err := manager.GetCredentialsByProvider()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed: %v\n", err)
		os.Exit(1)
	}

	envVars := make(map[string]string)

	// Map tokens to environment variables
	for i, token := range creds.OpenAI {
		if token.IsValid {
			envVars[fmt.Sprintf("OPENAI_API_KEY_%d", i+1)] = token.Value
		}
	}

	for i, token := range creds.Gemini {
		if token.IsValid {
			envVars[fmt.Sprintf("GEMINI_API_KEY_%d", i+1)] = token.Value
		}
	}

	for i, token := range creds.Nvidia {
		if token.IsValid {
			envVars[fmt.Sprintf("NVIDIA_API_KEY_%d", i+1)] = token.Value
		}
	}

	for i, token := range creds.GitHub {
		if token.IsValid {
			envVars[fmt.Sprintf("GITHUB_TOKEN_%d", i+1)] = token.Value
		}
	}

	for i, token := range creds.Vercel {
		if token.IsValid {
			envVars[fmt.Sprintf("VERCEL_TOKEN_%d", i+1)] = token.Value
		}
	}

	// Output as JSON (for piping to other commands)
	output, _ := json.MarshalIndent(envVars, "", "  ")
	fmt.Println(string(output))
}

func cmdRefresh(manager *credential.CredentialManager) {
	fmt.Println("🔄 Refreshing token cache...")
	manager.ClearCache()
	fmt.Println("Cache cleared")
}

func cmdSummary(manager *credential.CredentialManager) {
	fmt.Println("📋 Token Summary")
	fmt.Println()

	summary, err := manager.GetSummary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed: %v\n", err)
		os.Exit(1)
	}

	data, _ := json.MarshalIndent(summary, "", "  ")
	fmt.Println(string(data))
}
