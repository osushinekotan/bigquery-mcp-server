import logging

from fastmcp import FastMCP

from servers.settings import APP_HOST, APP_PORT
from servers.tools.bigquery import mcp as bigquery_mcp
from servers.tools.health import mcp as health_mcp

logger = logging.getLogger(__name__)

server = FastMCP("servers")
server.mount("bigquery", bigquery_mcp)
server.mount("health", health_mcp)

if __name__ == "__main__":
    server.run(transport="sse", host=APP_HOST, port=APP_PORT)
