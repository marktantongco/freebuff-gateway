# Freebuff Gateway Demo

## Quick Start

```bash
# Make scripts executable
chmod +x scripts/demo.sh scripts/record-demo.sh

# Run interactive demo
./scripts/demo.sh

# Record a demo video
./scripts/record-demo.sh
```

## Demo Sections

### 1. Build & Start
The gateway compiles to a single 25MB binary and starts in under 1 second.

```bash
make build
./bin/freebuff-gateway
```

### 2. Health Check
Verify the gateway is running:

```bash
curl http://localhost:30080/healthz
```

### 3. Configuration
Demonstrate layered configuration:

```bash
# Default config
./bin/freebuff-gateway

# Override with env vars
LISTEN_ADDR=:8080 LOG_LEVEL=debug ./bin/freebuff-gateway

# Use config file
cp configs/config.example.json data/config.json
./bin/freebuff-gateway
```

### 4. API Key Management
Create and manage API keys:

```bash
# Create a key
./bin/authkeys create my-app

# List all keys
./bin/authkeys list

# Validate a key
./bin/authkeys validate sk-your-key-here
```

### 5. Model Registry
Query available models:

```bash
curl -H "Authorization: Bearer sk-your-key" \
     http://localhost:30080/v1/models
```

### 6. Chat Completion
Send an OpenAI-compatible request:

```bash
curl -X POST http://localhost:30080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-your-key" \
  -d '{
    "model": "gpt-4",
    "messages": [
      {"role": "user", "content": "Hello!"}
    ]
  }'
```

### 7. Web Dashboard
Open the admin dashboard:

```bash
open http://localhost:30080
```

Features:
- Account management
- Session monitoring
- Proxy pool status
- System logs
- Configuration UI

### 8. Docker Deployment
Deploy with Docker:

```bash
# Build
docker build -t freebuff-gateway .

# Run
docker run -p 30080:30080 \
  -e ADMIN_PASSWORD=secret \
  freebuff-gateway

# Or use docker-compose
docker-compose up -d
```

## Recording a Demo Video

### Option 1: asciinema (Recommended)

```bash
# Install asciinema
pip install asciinema

# Record
asciinema rec demo.cast -c "./scripts/demo.sh"

# Play back
asciinema play demo.cast

# Share online
asciinema upload demo.cast
```

### Option 2: Terminal GIF with agg

```bash
# Install agg
cargo install agg

# Record with asciinema first
asciinema rec demo.cast -c "./scripts/demo.sh"

# Convert to GIF
agg demo.cast demo.gif
```

### Option 3: OBS Studio

1. Open OBS Studio
2. Add a "Window Capture" source
3. Select your terminal window
4. Run the demo: `./scripts/demo.sh`
5. Record and stop when done

### Option 4: macOS QuickTime

1. Open QuickTime Player
2. File → New Screen Recording
3. Select terminal area
4. Run the demo
5. Stop recording

## Demo Script Features

The `scripts/demo.sh` script demonstrates:

1. **Build Process** - Compiling the Go binary
2. **Health Checks** - Verifying the gateway is running
3. **API Key Management** - Creating and validating keys
4. **Model Registry** - Querying available models
5. **Chat Completions** - Sending OpenAI-compatible requests
6. **Web Dashboard** - Showing the admin interface
7. **Configuration** - Environment variable overrides
8. **Performance** - Resource usage and latency

## Tips for a Great Demo

1. **Use a clean terminal** - Clear history before recording
2. **Large font** - Make text readable in video
3. **Good lighting** - If showing face cam
4. **Narrate** - Explain what's happening
5. **Show results** - Don't just show commands, show output
6. **Keep it short** - 2-3 minutes is ideal
7. **Show the web UI** - Visual dashboards are impressive

## Sample Recording Command

```bash
# Full demo with asciinema
asciinema rec -c "./scripts/demo.sh" demo_$(date +%Y%m%d).cast
```

## Troubleshooting

### Gateway won't start
```bash
# Check if port is in use
lsof -i :30080

# Kill existing process
pkill -f freebuff-gateway

# Try again
./bin/freebuff-gateway
```

### No output from commands
```bash
# Check gateway logs
tail -f logs/gateway.log

# Verify gateway is running
curl http://localhost:30080/healthz
```

### Recording not working
```bash
# Check asciinema
asciinema --version

# Try with script instead
script -c "./scripts/demo.sh" demo.txt
```

## Full Demo Script

See `scripts/demo.sh` for the complete interactive demo.

See `scripts/record-demo.sh` for the automated recording version.
