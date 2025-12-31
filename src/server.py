"""MCP server for AWS Athena cost analysis."""

import asyncio
import json
import sys
from typing import Any, Sequence

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp.types import Tool, TextContent
except ImportError:
    # Fallback for different MCP SDK versions
    try:
        from mcp.server.models import Server
        from mcp.server.stdio import stdio_server
        from mcp.types import Tool, TextContent
    except ImportError:
        print("Error: MCP SDK not found. Please install with: pip install mcp", file=sys.stderr)
        sys.exit(1)

from .tools.fetch_queries import fetch_athena_queries
from .tools.analyze_cost import analyze_cost_increase
from .tools.compare_queries import compare_expensive_queries


# Create the MCP server instance
server = Server("aws-athena-cost")


@server.list_tools()
async def list_tools() -> list[Tool]:
    """List available tools."""
    return [
        Tool(
            name="fetch_athena_queries",
            description="Query Athena query execution data from PostgreSQL database and export to CSV. Note: Data is fetched from AWS Athena by a daily process (scripts/daily_fetch_queries.py), not by this tool.",
            inputSchema={
                "type": "object",
                "properties": {
                    "workgroup": {
                        "type": "string",
                        "description": "Athena workgroup name (optional - if not provided, queries all workgroups)"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date in YYYY-MM-DD format",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date in YYYY-MM-DD format",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "output_dir": {
                        "type": "string",
                        "description": "Output directory for CSV (optional, default: ./reports)"
                    }
                },
                "required": ["start_date", "end_date"]
            }
        ),
        Tool(
            name="analyze_cost_increase",
            description="Analyze cost increases by comparing baseline vs spike periods. Can query from PostgreSQL database or read from CSV file.",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "Path to CSV file with query data (optional - if not provided, queries PostgreSQL)"
                    },
                    "baseline_start": {
                        "type": "string",
                        "description": "Baseline period start date (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "baseline_end": {
                        "type": "string",
                        "description": "Baseline period end date (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "spike_start": {
                        "type": "string",
                        "description": "Spike period start date (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "spike_end": {
                        "type": "string",
                        "description": "Spike period end date (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "workgroup": {
                        "type": "string",
                        "description": "Optional workgroup filter for PostgreSQL query"
                    }
                },
                "required": ["baseline_start", "baseline_end", "spike_start", "spike_end"]
            }
        ),
        Tool(
            name="compare_expensive_queries",
            description="Compare expensive queries and extract patterns. Can query from PostgreSQL database or read from CSV file.",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_file": {
                        "type": "string",
                        "description": "Path to CSV file with query data (optional - if not provided, queries PostgreSQL)"
                    },
                    "query_pattern": {
                        "type": "string",
                        "description": "Optional pattern to filter queries (e.g., table name)"
                    },
                    "query_id": {
                        "type": "string",
                        "description": "Optional specific query execution ID to analyze"
                    },
                    "baseline_start": {
                        "type": "string",
                        "description": "Optional baseline start date for comparison (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "baseline_end": {
                        "type": "string",
                        "description": "Optional baseline end date for comparison (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "target_date": {
                        "type": "string",
                        "description": "Optional target date for comparison (YYYY-MM-DD)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "start_date": {
                        "type": "string",
                        "description": "Start date for PostgreSQL query (YYYY-MM-DD, required if csv_file not provided)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "end_date": {
                        "type": "string",
                        "description": "End date for PostgreSQL query (YYYY-MM-DD, required if csv_file not provided)",
                        "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                    },
                    "workgroup": {
                        "type": "string",
                        "description": "Optional workgroup filter for PostgreSQL query"
                    }
                },
                "required": []
            }
        )
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> Sequence[TextContent]:
    """Handle tool calls."""
    try:
        if name == "fetch_athena_queries":
            workgroup = arguments.get("workgroup")  # Optional - None means all workgroups
            start_date = arguments["start_date"]
            end_date = arguments["end_date"]
            output_dir = arguments.get("output_dir")
            
            result = fetch_athena_queries(
                workgroup=workgroup,
                start_date=start_date,
                end_date=end_date,
                output_dir=output_dir
            )
            
            if result["success"]:
                response = {
                    "success": True,
                    "file_path": result["file_path"],
                    "total_processed": result["total_processed"],
                    "matched_count": result["matched_count"],
                    "message": f"Successfully exported {result['matched_count']} queries to {result['file_path']}"
                }
            else:
                response = {
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                    "message": f"Failed to fetch queries: {result.get('error', 'Unknown error')}"
                }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2))]
        
        elif name == "analyze_cost_increase":
            csv_file = arguments.get("csv_file")
            baseline_start = arguments["baseline_start"]
            baseline_end = arguments["baseline_end"]
            spike_start = arguments["spike_start"]
            spike_end = arguments["spike_end"]
            workgroup = arguments.get("workgroup")
            
            result = analyze_cost_increase(
                csv_file=csv_file,
                baseline_start=baseline_start,
                baseline_end=baseline_end,
                spike_start=spike_start,
                spike_end=spike_end,
                workgroup=workgroup
            )
            
            if result["success"]:
                response = {
                    "success": True,
                    "analysis": result,
                    "message": "Cost analysis completed successfully"
                }
            else:
                response = {
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                    "message": f"Failed to analyze cost: {result.get('error', 'Unknown error')}"
                }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2, default=str))]
        
        elif name == "compare_expensive_queries":
            csv_file = arguments.get("csv_file")
            query_pattern = arguments.get("query_pattern")
            query_id = arguments.get("query_id")
            baseline_start = arguments.get("baseline_start")
            baseline_end = arguments.get("baseline_end")
            target_date = arguments.get("target_date")
            start_date = arguments.get("start_date")
            end_date = arguments.get("end_date")
            workgroup = arguments.get("workgroup")
            
            result = compare_expensive_queries(
                csv_file=csv_file,
                query_pattern=query_pattern,
                query_id=query_id,
                baseline_start=baseline_start,
                baseline_end=baseline_end,
                target_date=target_date,
                start_date=start_date,
                end_date=end_date,
                workgroup=workgroup
            )
            
            if result["success"]:
                response = {
                    "success": True,
                    "comparison": result,
                    "message": "Query comparison completed successfully"
                }
            else:
                response = {
                    "success": False,
                    "error": result.get("error", "Unknown error"),
                    "message": f"Failed to compare queries: {result.get('error', 'Unknown error')}"
                }
            
            return [TextContent(type="text", text=json.dumps(response, indent=2, default=str))]
        
        else:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": False,
                    "error": f"Unknown tool: {name}"
                }, indent=2)
            )]
    
    except Exception as e:
        return [TextContent(
            type="text",
            text=json.dumps({
                "success": False,
                "error": str(e),
                "message": f"Error executing tool {name}: {str(e)}"
            }, indent=2)
        )]


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options()
        )


if __name__ == "__main__":
    asyncio.run(main())

