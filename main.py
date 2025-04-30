import logging

from fastmcp import FastMCP

from servers.clients.bigquery import init_bigquery_client
from servers.settings import APP_HOST, APP_PORT
from servers.tools.bigquery import BigqueryTool
from servers.tools.health import health_check

logger = logging.getLogger(__name__)

bigquery_client = init_bigquery_client()
server = FastMCP("servers")

# Register the BigQuery tool
bigquery_tool = BigqueryTool(client=bigquery_client)
server.add_tool(bigquery_tool.list_datasets, tags=["bigquery"])
server.add_tool(bigquery_tool.get_allowed_datasets, tags=["bigquery"])
server.add_tool(bigquery_tool.list_tables, tags=["bigquery"])
server.add_tool(bigquery_tool.describe_table, tags=["bigquery"])
server.add_tool(bigquery_tool.execute_query, tags=["bigquery"])

# Register the health check tool
server.add_tool(health_check, tags=["system"], name="health_check")


if __name__ == "__main__":
    server.run(transport="sse", host=APP_HOST, port=APP_PORT)
