# bigquery-mcp-server

## Setup

```bash
uv sync
```

## MCP Server

**start sse server**

```bash
uv run main.py
```

or docker

```bash
docker compose up
```

**mcp inspector**

```bash
npx @modelcontextprotocol/inspector npm run dev
```

## MCP Client

### claude desktop

```
{
  "mcpServers": {
    "servers": {
      "command": "uvx",
      "args": ["mcp-proxy", "http://localhost:8001/sse"]
    }
  }
}
```
