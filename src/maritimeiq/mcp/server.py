"""MCP server entrypoint (single process, three tool namespaces)."""
from __future__ import annotations

from maritimeiq.mcp.tools import MCPGateway
from maritimeiq.spark.session import get_spark


def create_gateway() -> MCPGateway:
    return MCPGateway(get_spark())


def main() -> None:
    """Lightweight stdio-style loop for demos without a full MCP transport."""
    gw = create_gateway()
    print("MaritimeIQ MCP gateway ready. Namespaces: lakehouse, reference, review")
    print(gw.lakehouse.list_tables("bronze"))
    print(gw.lakehouse.list_tables("silver"))


if __name__ == "__main__":
    main()
