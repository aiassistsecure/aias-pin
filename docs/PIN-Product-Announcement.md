# Introducing PIN: The People's AI Network

## AI Should Be Accessible to Everyone. Now It Is.

---

## The Problem

**AI is broken.**

Not the technology—the economics.

Today, accessing powerful AI models means:

- **Paying premium prices** to Big Tech gatekeepers ($20/month minimums, usage caps, surprise bills)
- **Waiting in queues** during peak hours while models are "at capacity"
- **Trusting corporations** with your private prompts and sensitive data
- **Accepting censorship** and arbitrary content policies you didn't agree to

Meanwhile, millions of GPUs sit idle around the world. Gamers, researchers, hobbyists, and small businesses own incredible compute power that's utilized less than 10% of the time.

**The math doesn't add up.**

Users overpay. GPU owners underutilize. Big Tech takes the spread.

---

## The Solution

**PIN: P2P Inference Network**

A decentralized AI marketplace that connects you directly to GPU operators worldwide.

No middleman. No gatekeepers. No artificial scarcity.

```
┌──────────┐                      ┌──────────┐
│   You    │ ←── AI Response ──── │  GPU     │
│  (User)  │ ──── Request ──────→ │  Owner   │
└──────────┘                      └──────────┘
              Direct Connection
              90% to Operator
              No Big Tech Markup
```

---

## How It Works

### For Users

1. **Choose your model** – Llama, Mistral, CodeLlama, and more
2. **Send your prompt** – Through our API or chat interface
3. **Get instant responses** – Routed to the fastest available node
4. **Pay per use** – Only for tokens you actually consume

No subscriptions. No commitments. No surprise charges.

### For GPU Owners

1. **Install the daemon** – One config file, one command
2. **Connect your hardware** – Ollama, vLLM, or any OpenAI-compatible server
3. **Get auto-verified** – Quality interviews ensure fair routing
4. **Start earning** – 90% of every request goes directly to you

Your GPU. Your earnings. Your rules.

---

## Why PIN Wins

| Traditional AI | PIN |
|----------------|-----|
| $20+/month subscriptions | Pay only for what you use |
| Centralized servers | Distributed global network |
| "At capacity" errors | Always-on availability |
| Data sent to corporations | Direct encrypted connections |
| One-size-fits-all models | Choose from 100+ models |
| Corporate content policies | Uncensored responses |

---

## The Numbers

**For Users:**
- Up to **70% cheaper** than OpenAI/Anthropic
- **< 500ms** average response time
- **100+ models** available globally
- **Zero** monthly minimums

**For Operators:**
- **67%+ revenue share** – we take up to 33% to keep the network running, and we're committed to lowering our share as we scale
- **$0.05 - $0.50** per 1,000 tokens (you set the price)
- **USDT payouts** directly to your wallet
- **No upfront costs** to join

*We see operators as partners, not vendors. As PIN grows, operator revenue share grows with it.*

---

## Built for Trust

**Quality Verified**

Every node undergoes automated quality interviews. We test accuracy, speed, and reliability before routing production traffic. No junk responses. No hallucinating models. Just verified AI.

**Proof of Response**

Unlike blockchain networks that waste compute on meaningless puzzles, PIN only pays for real work. You pay when you get a response. Operators earn when they deliver one.

**No Exposed Endpoints**

Operators never expose their LLM servers to the public internet. All traffic flows through authenticated WebSocket tunnels. Your home GPU stays behind your firewall.

---

## Use Cases

**Developers**
- Drop-in OpenAI-compatible API
- Same code, different backend
- No vendor lock-in

**Businesses**
- Cost-effective AI at scale
- Choose operators in your region
- Enterprise SLAs available

**Researchers**
- Access uncensored models
- Run experiments affordably
- No rate limits on creativity

**Privacy-Conscious Users**
- No prompt logging by Big Tech
- Choose operators you trust
- Self-host and earn while you use

---

## Get Started in 60 Seconds

### As a User

```bash
curl https://api.aiassist.net/v1/chat/completions \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama3.2",
    "messages": [{"role": "user", "content": "Hello, PIN!"}]
  }'
```

### As an Operator

```json
{
  "clientId": "op_your_id",
  "apiSecret": "your_secret",
  "nodes": [{
    "alias": "my-gpu",
    "inferenceUri": "http://localhost:11434",
    "apiMode": "ollama",
    "region": "us-east",
    "capacity": 10
  }]
}
```

```bash
./pin-clientd --config config.json
```

That's it. You're in the network.

---

## The Vision

We're not building another AI company.

We're building **AI infrastructure for the people**.

A world where:
- Anyone can access cutting-edge AI without Big Tech subscriptions
- GPU owners monetize their idle compute
- Developers build without platform risk
- Privacy is the default, not the exception

**PIN is the Airbnb of AI compute.**

You don't need a data center to participate in the AI economy. You need a GPU and an internet connection.

---

## Join the Network

**Users:** [aiassist.net/pin](https://aiassist.net/pin) – Start using AI today

**Operators:** [aiassist.net/pin/join](https://aiassist.net/pin/join) – Turn your GPU into income

**Developers:** [docs.aiassist.net/pin](https://docs.aiassist.net/pin) – API documentation

---

## FAQ

**Is this legal?**
Yes. Operators run open-source models on hardware they own. Users pay for compute, not copyrighted content.

**What models are available?**
Llama 3.2, Mistral, CodeLlama, Phi, Gemma, and 100+ more. Any model that runs on Ollama or OpenAI-compatible servers.

**How do I know I'll get quality responses?**
Every node is quality-verified through automated interviews. Only nodes that pass accuracy and speed thresholds receive production traffic.

**What if an operator goes offline mid-request?**
Automatic failover. Your request is instantly rerouted to the next available node. You only pay for completed responses.

**How do operators get paid?**
USDT on BSC (Binance Smart Chain). Minimum withdrawal $10. Weekly payout processing.

**What's the revenue split?**
Operators keep 67% or more of every request. We take up to 33% for network infrastructure, routing, and quality assurance. As PIN scales and costs decrease, we'll pass more back to operators.

**Is my data private?**
PIN doesn't log prompts or responses. Traffic flows directly between you and the operator through encrypted channels.

---

## The Future is Distributed

Centralized AI had its moment.

The future is **millions of GPUs, owned by millions of people, serving millions of users**.

No gatekeepers. No artificial scarcity. No permission required.

**Welcome to PIN.**

---

*PIN is part of AiAssist Secure – Security-first AI infrastructure for the next generation.*
