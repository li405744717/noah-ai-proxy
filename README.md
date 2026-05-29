# noah-ai-proxy

OpenAI-compatible API proxy that bridges requests to Agent Gateway sidecar via authorized session path.

## Architecture

```
openhanako / LiteLLM client
        │
        ▼
noah-ai-proxy (127.0.0.1:4000)   ← OpenAI-compatible API
        │
        ▼
Agent Gateway Sidecar (127.0.0.1:4319)   ← Internal authorized path
        │
        ▼
Claude (via Bedrock)   ← No extra API cost
```

## Why this approach?

- **No public exposure**: Binds only to `127.0.0.1`, invisible to external network scans
- **Uses authorized channel**: Requests flow through the same Agent Gateway session path that's already approved by InfoSec
- **No credential storage**: No AWS AK/SK needed locally — the sidecar handles authentication
- **OpenAI-compatible**: Any client that speaks OpenAI API format (LiteLLM, openhanako, etc.) can connect directly

## Quick Start

```bash
cd noah-ai-proxy
npm install
node index.js
```

## Configuration

Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `PROXY_PORT` | `4000` | Port to listen on |
| `PROXY_HOST` | `127.0.0.1` | Bind address (keep as localhost!) |
| `SIDECAR_URL` | `http://127.0.0.1:4319` | Agent Gateway sidecar URL |
| `SIDECAR_API_KEY` | (empty) | Sidecar API key if configured |
| `PROXY_API_KEY` | (empty) | Optional API key for this proxy |
| `WORKSPACE_PATH` | `/var/lib/agent-gw/sessions/proxy-workspace` | Workspace for agent sessions |
| `CLAUDE_HOME_PATH` | auto | Claude home directory |
| `PERMISSION_MODE` | `acceptEdits` | Agent permission mode |
| `SESSION_MODE` | `one_shot` | `one_shot` or `persistent` |
| `TIMEOUT_MS` | `120000` | Request timeout in ms |

## Usage with openhanako / LiteLLM

Configure your client to use:

```
base_url: http://127.0.0.1:4000/v1
model: claude-agent
api_key: (your PROXY_API_KEY if set, otherwise any string)
```

### LiteLLM config example (`litellm_config.yaml`):

```yaml
model_list:
  - model_name: claude-agent
    litellm_params:
      model: openai/claude-agent
      api_base: http://127.0.0.1:4000/v1
      api_key: "sk-noah-proxy"
```

### Direct curl test:

```bash
curl http://127.0.0.1:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "claude-agent",
    "messages": [{"role": "user", "content": "Hello"}],
    "stream": false
  }'
```

## Endpoints

- `GET /health` — Health check
- `GET /v1/models` — List available models
- `POST /v1/chat/completions` — Chat completions (stream & non-stream)

## Security Notes

- **NEVER change `PROXY_HOST` to `0.0.0.0`** — this would expose the proxy to the network
- If running on a shared machine, set `PROXY_API_KEY` to prevent unauthorized local access
- Do NOT register this service in any Cloudflare Tunnel or reverse proxy routes
