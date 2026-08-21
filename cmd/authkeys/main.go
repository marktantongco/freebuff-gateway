// Command authkeys provides CLI management for API keys.
//
// Usage:
//
//	authkeys create <name>       # Create a new API key
//	authkeys list                # List all API keys
//	authkeys delete <id>         # Delete an API key
//	authkeys validate <key>      # Validate an API key
package main

import (
	"database/sql"
	"fmt"
	"os"
	"text/tabwriter"

	_ "modernc.org/sqlite"

	"github.com/marktantongco/freebuff-gateway/internal/authkeys"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	dbPath := getEnv("DB_PATH", "./data/gateway.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to open database: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	repo := authkeys.NewRepo(db)

	switch os.Args[1] {
	case "create":
		cmdCreate(repo, os.Args[2:])
	case "list":
		cmdList(repo)
	case "delete":
		cmdDelete(repo, os.Args[2:])
	case "validate":
		cmdValidate(repo, os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("Freebuff Gateway — API Key Management")
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Println("  authkeys create <name>       Create a new API key")
	fmt.Println("  authkeys list                List all API keys")
	fmt.Println("  authkeys delete <id>         Delete an API key")
	fmt.Println("  authkeys validate <key>      Validate an API key")
}

func cmdCreate(repo *authkeys.Repo, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: authkeys create <name>\n")
		os.Exit(1)
	}

	name := args[0]
	record, err := repo.Create(name)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to create key: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("✅ API Key created successfully!")
	fmt.Printf("   ID:    %s\n", record.ID)
	fmt.Printf("   Name:  %s\n", record.Name)
	fmt.Printf("   Key:   %s\n", record.Key)
	fmt.Println()
	fmt.Println("⚠️  Save this key! It won't be shown again.")
}

func cmdList(repo *authkeys.Repo) {
	records, err := repo.List()
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to list keys: %v\n", err)
		os.Exit(1)
	}

	if len(records) == 0 {
		fmt.Println("No API keys found")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "ID\tNAME\tPREFIX\tACTIVE\tLAST USED\n")
	fmt.Fprintf(w, "---\t----\t------\t------\t----------\n")

	for _, rec := range records {
		active := "✓"
		if !rec.IsActive {
			active = "✗"
		}
		lastUsed := "never"
		if rec.LastUsedAt > 0 {
			lastUsed = fmt.Sprintf("%d", rec.LastUsedAt)
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\n",
			rec.ID, rec.Name, rec.KeyPrefix, active, lastUsed)
	}

	w.Flush()
	fmt.Printf("\nTotal: %d keys\n", len(records))
}

func cmdDelete(repo *authkeys.Repo, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: authkeys delete <id>\n")
		os.Exit(1)
	}

	id := args[0]
	if err := repo.Delete(id); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Failed to delete key: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("✅ Key %s deleted\n", id)
}

func cmdValidate(repo *authkeys.Repo, args []string) {
	if len(args) < 1 {
		fmt.Fprintf(os.Stderr, "Usage: authkeys validate <key>\n")
		os.Exit(1)
	}

	key := args[0]
	record, err := repo.Authenticate(key)
	if err != nil {
		fmt.Printf("❌ Invalid key\n")
		os.Exit(1)
	}

	fmt.Printf("✅ Valid key\n")
	fmt.Printf("   ID:     %s\n", record.ID)
	fmt.Printf("   Name:   %s\n", record.Name)
	fmt.Printf("   Active: %v\n", record.IsActive)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
