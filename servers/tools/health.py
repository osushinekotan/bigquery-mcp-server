from fastmcp import FastMCP

mcp = FastMCP("health")


@mcp.tool(name="check", tags=["health"])
def check() -> str:
    """
    Health check endpoint.
    """
    return "health ✅"
