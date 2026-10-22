# PIN Operator Guide

Welcome to the P2P Inference Network (PIN). This guide will help you set up and manage your inference nodes to earn USDT by providing AI compute power to the network.

---

## Table of Contents

1. [Overview](#overview)
2. [Requirements](#requirements)
3. [Getting Started](#getting-started)
4. [Node Configuration](#node-configuration)
5. [Running the Daemon](#running-the-daemon)
6. [Quality Interviews](#quality-interviews)
7. [Managing Multiple Nodes](#managing-multiple-nodes)
8. [Earnings & Payouts](#earnings--payouts)
9. [Troubleshooting](#troubleshooting)

---

## Overview

As a PIN Operator, you provide AI inference capacity to the network. When users request AI completions, their requests are routed to your nodes. You earn **90% of every inference request** processed.

**How it works:**
1. You run an LLM server (Ollama, vLLM, LMStudio, etc.)
2. You register as an operator on AiAssist Secure
3. You download and configure the PIN daemon
4. The daemon connects and auto-registers your nodes
5. Your nodes are quality-tested automatically
6. Requests flow in, you earn USDT

---

## Requirements

### Hardware
- GPU recommended (NVIDIA with CUDA support)
- Minimum 16GB RAM
- Stable internet connection

### Software

| Backend | API Mode | Default Port |
|---------|----------|--------------|
| Ollama | `ollama` | 11434 |
| vLLM | `openai` | 8000 |
| LMStudio | `openai` | 1234 |
| LocalAI | `openai` | 8080 |
| text-generation-inference | `openai` | 8080 |

### What You'll Receive
- **Operator ID**: Your unique identifier (`op_xxx`)
- **API Secret**: Authentication key (shown once!)

---

## Getting Started

### Step 1: Set Up Your LLM Server

**Ollama:**
```bash
curl -fsSL https://ollama.ai/install.sh | sh
ollama pull llama3.2
ollama serve
```

**vLLM:**
```bash
python -m vllm.entrypoints.openai.api_server \
  --model meta-llama/Llama-3.2-3B-Instruct \
  --port 8000
```

### Step 2: Register as an Operator

1. Go to [aiassist.net/pin/join](https://aiassist.net/pin/join)
2. Enter your operator name
3. Click **"Become an Operator"**
4. **Save your credentials immediately** - the API secret is shown only once!

### Step 3: Configure the Daemon

Create `config.json`:

```json
{
  "clientId": "op_your_operator_id",
  "apiSecret": "your_api_secret",
  "payoutAddress": "0x_your_bsc_wallet_address",
  "nodes": [
    {
      "alias": "GPU-1",
      "inferenceUri": "http://localhost:11434",
      "apiMode": "ollama",
      "region": "us-east",
      "capacity": 10
    }
  ]
}
```

The `payoutAddress` is your BSC (BNB Chain) wallet where you'll receive USDT earnings. You can set it during registration or update it anytime via the config file - the daemon will sync it on connect.

### Step 4: Run the Daemon

```bash
./pin-clientd --config config.json
```

You'll see:
```
     █████╗ ██╗ █████╗ ███████╗    ██████╗ ██╗███╗   ██╗
    ██╔══██╗██║██╔══██╗██╔════╝    ██╔══██╗██║████╗  ██║
    ...
    PIN Client Daemon v2.1.0 - https://AiAssist.net

INFO Operator ID: op_abc123
INFO Nodes configured: 1
INFO   - GPU-1 | http://localhost:11434 | ollama | capacity: 10
INFO Connecting to PIN server...
INFO Authenticated! Operator: op_abc123
INFO Registering node: GPU-1 (region: us-east, capacity: 10)
INFO Node GPU-1 has 3 models: ["llama3.2:latest", "mistral:7b", "codellama:13b"]
INFO [NODE] REGISTERED GPU-1 (ID: pin_node_xyz) with 3 models
```

---

## Node Configuration

### Required Fields

Every node must specify all fields:

| Field | Description | Example |
|-------|-------------|---------|
| `alias` | Friendly name | `"GPU-A100"` |
| `inferenceUri` | LLM server URL | `"http://localhost:11434"` |
| `apiMode` | API format | `"ollama"` or `"openai"` |
| `region` | Your location | `"us-east"` |
| `capacity` | Max concurrent requests | `10` |

### Regions

| ID | Name | Use For |
|----|------|---------|
| `us-east` | US East | Virginia, NYC, AWS us-east-1 |
| `us-west` | US West | California, Oregon, AWS us-west-2 |
| `eu-west` | EU West | Ireland, UK, AWS eu-west-1 |
| `eu-central` | EU Central | Frankfurt, Amsterdam |
| `asia-pacific` | Asia Pacific | Tokyo, Singapore |
| `global` | Global | Unknown location |

### API Modes

**Ollama** (`apiMode: "ollama"`):
- Standard Ollama installations
- Uses `/api/tags` for model discovery
- Uses `/api/chat` for inference

**OpenAI** (`apiMode: "openai"`):
- vLLM, LMStudio, LocalAI, TGI
- Uses `/v1/models` for model discovery
- Uses `/v1/chat/completions` for inference

---

## Running the Daemon

### Basic Usage

```bash
./pin-clientd --config config.json
```

### With Debug Logging

```bash
RUST_LOG=debug ./pin-clientd --config config.json
```

### As a System Service

```bash
sudo mkdir -p /opt/pin-clientd
sudo cp pin-clientd /opt/pin-clientd/
sudo cp config.json /opt/pin-clientd/

# Create service file
sudo tee /etc/systemd/system/pin-clientd.service << EOF
[Unit]
Description=PIN Client Daemon
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/pin-clientd
ExecStart=/opt/pin-clientd/pin-clientd --config config.json
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable pin-clientd
sudo systemctl start pin-clientd
```

### View Service Logs

```bash
journalctl -u pin-clientd -f
```

---

## Quality Interviews

When your node connects, the server automatically tests its quality.

### What Happens

1. Server sends test prompts to your node
2. Daemon runs them against your LLM
3. Daemon reports response times and accuracy
4. Server assigns a quality tier

### Quality Tiers

| Tier | Requirements | Effect |
|------|--------------|--------|
| `verified` | >90% accuracy, >20 tok/s | Highest routing priority |
| `standard` | >70% accuracy, >10 tok/s | Normal routing |
| `slow` | >70% accuracy, <10 tok/s | Budget tier only |
| `failed` | <70% accuracy | Blocked from production |

### Interview Output

```
INFO [INTERVIEW] Received interview for GPU-1 - model llama3.2:latest (5 prompts)
INFO [INTERVIEW] Running prompt 1/5: factual_1
INFO [INTERVIEW] Running prompt 2/5: instruction_1
...
INFO =====================================
INFO [INTERVIEW] Quality Tier Assigned for GPU-1!
INFO   Tier: VERIFIED
INFO   Accuracy: 94.2%
INFO   Speed: 28.5 tokens/sec
INFO   Reason: High accuracy and fast response times
INFO =====================================
```

### Retry Policy

- Failed interviews can be retried after 1 hour
- Maximum 3 attempts per 24 hours
- Reconnecting the daemon triggers a new interview

---

## Managing Multiple Nodes

### Mixed Backends

You can run different LLM backends from one daemon:

```json
{
  "clientId": "op_abc123",
  "apiSecret": "secret_xyz",
  "nodes": [
    {
      "alias": "ollama-local",
      "inferenceUri": "http://localhost:11434",
      "apiMode": "ollama",
      "region": "us-east",
      "capacity": 5
    },
    {
      "alias": "vllm-server",
      "inferenceUri": "http://localhost:8000",
      "apiMode": "openai",
      "region": "us-east",
      "capacity": 20
    },
    {
      "alias": "remote-gpu",
      "inferenceUri": "http://192.168.1.100:11434",
      "apiMode": "ollama",
      "region": "us-east",
      "capacity": 10
    }
  ]
}
```

### Multiple Daemons

For nodes on different machines, run separate daemons with the same operator credentials:

**Machine A:**
```json
{
  "clientId": "op_abc123",
  "apiSecret": "secret_xyz",
  "nodes": [
    { "alias": "machine-a", "inferenceUri": "http://localhost:11434", ... }
  ]
}
```

**Machine B:**
```json
{
  "clientId": "op_abc123",
  "apiSecret": "secret_xyz",
  "nodes": [
    { "alias": "machine-b", "inferenceUri": "http://localhost:11434", ... }
  ]
}
```

Both nodes register under the same operator account.

---

## Earnings & Payouts

### How Earnings Work

- You earn **67% or more** of every inference request (up to 33% protocol fee)
- As the network scales, operator revenue share increases
- Earnings calculated based on tokens processed
- Only successful completions are billed

### Checking Earnings

Your dashboard shows:
- **Current Balance**: Available for withdrawal
- **Total Earned**: Lifetime earnings
- **24h / 7d / 30d**: Recent breakdowns

### Requesting a Payout

1. Go to Operator Dashboard
2. Add your BSC wallet address in Settings
3. Click **Request Withdrawal**
4. Enter amount (minimum $10)
5. Submit

Payouts are in USDT on BSC (Binance Smart Chain).

---

## Troubleshooting

### Node Shows "OFFLINE"

1. Check if LLM server is running:
   ```bash
   # Ollama
   curl http://localhost:11434/api/version
   
   # OpenAI-compatible
   curl http://localhost:8000/v1/models
   ```

2. Check daemon logs for errors
3. Restart the daemon

### Interview Failed

- Ensure your LLM is responding correctly
- Check for timeout errors in logs
- Try with a smaller/faster model first
- Wait 1 hour and reconnect to retry

### "No models found"

- Pull at least one model: `ollama pull llama3.2`
- Check your `inferenceUri` is correct
- Verify `apiMode` matches your backend

### Connection Keeps Dropping

- Check internet stability
- Verify firewall allows outbound WebSocket (port 443)
- Check for memory/resource issues on LLM server

### Models Not Showing in Network

- Models are discovered on node registration
- Restart daemon after pulling new models
- Check model names match expected format

---

## Support

For help, visit the community forums or contact support.

**Happy Earning!**
