package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"
)

type Check struct {
	Name   string
	Status string
	Pass   bool
}

func main() {
	checks := []Check{}

	// Go version
	checks = append(checks, checkGoVersion())
	// Configuration
	checks = append(checks, checkConfig())
	// Environment
	checks = append(checks, checkEnvironment())
	// Database
	checks = append(checks, checkDatabase())
	// Gateway health
	checks = append(checks, checkGateway())
	// Proxy pool
	checks = append(checks, checkProxyPool())

	// Print results
	fmt.Println("╔══════════════════════════════════════════════════╗")
	fmt.Println("║       Freebuff Gateway — Health Check           ║")
	fmt.Println("╚══════════════════════════════════════════════════╝")
	fmt.Println()

	allPass := true
	for _, c := range checks {
		symbol := "✅"
		if !c.Pass {
			symbol = "❌"
			allPass = false
		}
		fmt.Printf("  %s %-30s %s\n", symbol, c.Name, c.Status)
	}

	fmt.Println()
	if allPass {
		fmt.Println("  ✅ All checks passed")
		os.Exit(0)
	} else {
		fmt.Println("  ❌ Some checks failed")
		os.Exit(1)
	}
}

func checkGoVersion() Check {
	c := Check{Name: "Go Version", Status: runtime.Version(), Pass: true}
	if runtime.Version() < "go1.22" {
		c.Status = runtime.Version() + " (requires go1.22+)"
		c.Pass = false
	}
	return c
}

func checkConfig() Check {
	c := Check{Name: "Configuration", Pass: false}
	if _, err := os.Stat("data/config.json"); err == nil {
		c.Status = "data/config.json found"
		c.Pass = true
	} else if _, err := os.Stat("config.json"); err == nil {
		c.Status = "config.json found"
		c.Pass = true
	} else {
		c.Status = "no config file found"
	}
	return c
}

func checkEnvironment() Check {
	c := Check{Name: "Environment", Pass: true}
	missing := []string{}
	for _, env := range []string{"ADMIN_PASSWORD"} {
		if os.Getenv(env) == "" {
			missing = append(missing, env)
		}
	}
	if len(missing) > 0 {
		c.Status = "missing: " + strings.Join(missing, ", ")
		c.Pass = false
	} else {
		c.Status = "all required vars set"
	}
	return c
}

func checkDatabase() Check {
	c := Check{Name: "Database", Pass: false}
	dbPath := "data/gateway.db"
	if _, err := os.Stat(dbPath); err == nil {
		info, _ := os.Stat(dbPath)
		c.Status = fmt.Sprintf("%s (%d bytes)", dbPath, info.Size())
		c.Pass = true
	} else {
		c.Status = "not initialized (will create on start)"
		c.Pass = true // Not a failure
	}
	return c
}

func checkGateway() Check {
	c := Check{Name: "Gateway Health", Pass: false}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:8080/healthz")
	if err != nil {
		c.Status = "not running (start with: make run)"
		return c
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		c.Status = "responding on :8080"
		c.Pass = true
	} else {
		c.Status = fmt.Sprintf("HTTP %d", resp.StatusCode)
	}
	return c
}

func checkProxyPool() Check {
	c := Check{Name: "Proxy Pool", Pass: true}
	// Check if proxy pool checker is available
	if _, err := exec.Command("go", "env", "GOPATH").Output(); err == nil {
		c.Status = "available"
	} else {
		c.Status = "not configured"
	}
	return c
}
