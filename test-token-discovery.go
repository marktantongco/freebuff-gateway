package main

import (
	"fmt"
	"os"

	"github.com/marktantongco/freebuff-gateway/internal/credential"
)

func main() {
	fmt.Println("🔐 Freebuff Gateway — Token Discovery Test")
	fmt.Println("==========================================")
	fmt.Println()

	// Create discovery with default config
	config := credential.DefaultDiscoveryConfig()
	discovery := credential.NewTokenDiscovery(config)

	// Scan for tokens
	fmt.Println("🔍 Scanning secure-tokens directory...")
	if err := discovery.Scan(); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Scan failed: %v\n", err)
		os.Exit(1)
	}

	// Get all tokens
	tokens := discovery.GetTokens()
	fmt.Printf("✅ Found %d encrypted tokens\n\n", len(tokens))

	// Group by provider
	providerCounts := make(map[credential.ProviderType]int)
	for _, token := range tokens {
		providerCounts[token.Provider]++
	}

	fmt.Println("📊 Tokens by provider:")
	for provider, count := range providerCounts {
		fmt.Printf("  %s: %d tokens\n", provider, count)
	}
	fmt.Println()

	// Create manager and try to decrypt a few tokens
	managerConfig := credential.DefaultManagerConfig()
	manager := credential.NewCredentialManager(discovery, managerConfig)

	fmt.Println("🔓 Testing decryption...")
	decrypted := 0
	failed := 0

	for _, token := range discovery.SortTokensByProvider() {
		decryptedToken, err := manager.GetToken(token.ID)
		if err != nil {
			fmt.Printf("  ❌ %s: %v\n", token.ID, err)
			failed++
			continue
		}

		if decryptedToken.IsValid {
			fmt.Printf("  ✅ %s (%s): valid, hash=%s\n", token.ID, token.Provider, decryptedToken.ValueHash)
			decrypted++
		} else {
			fmt.Printf("  ⚠️  %s (%s): placeholder value\n", token.ID, token.Provider)
			decrypted++
		}
	}

	fmt.Println()
	fmt.Printf("Results: %d decrypted, %d failed\n", decrypted, failed)

	// Get summary
	summary, err := manager.GetSummary()
	if err == nil {
		fmt.Println()
		fmt.Println("📋 Summary:")
		fmt.Printf("  Total discovered: %d\n", summary.TotalDiscovered)
		fmt.Printf("  Total decrypted:  %d\n", summary.TotalDecrypted)
		fmt.Printf("  Total valid:      %d\n", summary.TotalValid)
	}
}
