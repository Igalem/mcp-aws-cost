"""AI Agent for AWS Athena Analytics using OpenAI-compatible API (e.g. Ollama) and MCP tools."""

import os
import json
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import traceback

# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv not installed, skip loading .env file

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

from src.tools.fetch_queries import fetch_athena_queries
from src.tools.analyze_cost import analyze_cost_increase
from src.tools.compare_queries import compare_expensive_queries


class AthenaAnalyticsAgent:
    """AI Agent that uses OpenAI-compatible API to interact with users and call MCP tools."""
    
    def __init__(self):
        self.client = None
        
        # Configure OpenAI client for local LLM (e.g., Ollama)
        # Default to localhost:11434/v1 for Ollama if not specified
        base_url = os.getenv("LLM_BASE_URL", "http://localhost:11434/v1")
        api_key = os.getenv("LLM_API_KEY", "ollama") # Ollama doesn't require key, but client might
        self.model_name = os.getenv("LLM_MODEL_NAME", "llama3.1") # Default to llama3.1 which supports tools
        
        if OpenAI:
            try:
                self.client = OpenAI(
                    base_url=base_url,
                    api_key=api_key
                )
                print(f"Initialized OpenAI client with base_url={base_url}, model={self.model_name}")
            except Exception as e:
                print(f"Failed to initialize OpenAI client: {e}")
                self.client = None
        
        # Define available tools for OpenAI format
        self.tools = [
            {
                "type": "function",
                "function": {
                    "name": "fetch_athena_queries",
                    "description": "Query Athena query execution data from PostgreSQL database and export to CSV. Use this when the user wants to fetch, export, or get query data.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "workgroup": {
                                "type": "string",
                                "description": "Athena workgroup name (optional - if not provided, queries all workgroups)"
                            },
                            "start_date": {
                                "type": "string",
                                "description": "Start date in YYYY-MM-DD format",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "end_date": {
                                "type": "string",
                                "description": "End date in YYYY-MM-DD format",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            }
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "analyze_cost_increase",
                    "description": "Analyze cost increases by comparing baseline vs spike periods. Use this when the user asks about cost spikes, cost increases, or wants to compare two time periods.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "baseline_start": {
                                "type": "string",
                                "description": "Baseline period start date (YYYY-MM-DD)",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "baseline_end": {
                                "type": "string",
                                "description": "Baseline period end date (YYYY-MM-DD)",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "spike_start": {
                                "type": "string",
                                "description": "Spike period start date (YYYY-MM-DD)",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "spike_end": {
                                "type": "string",
                                "description": "Spike period end date (YYYY-MM-DD)",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "workgroup": {
                                "type": "string",
                                "description": "Optional workgroup filter"
                            }
                        },
                        "required": ["baseline_start", "baseline_end", "spike_start", "spike_end"]
                    }
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "compare_expensive_queries",
                    "description": "Compare expensive queries and extract patterns. Use this when the user asks about expensive queries, query patterns, or wants to analyze query performance.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "start_date": {
                                "type": "string",
                                "description": "Start date for query analysis (YYYY-MM-DD)",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "end_date": {
                                "type": "string",
                                "description": "End date for query analysis (YYYY-MM-DD)",
                                "pattern": r"^\d{4}-\d{2}-\d{2}$"
                            },
                            "workgroup": {
                                "type": "string",
                                "description": "Optional workgroup filter"
                            },
                            "query_pattern": {
                                "type": "string",
                                "description": "Optional pattern to filter queries (e.g., table name)"
                            }
                        },
                        "required": ["start_date", "end_date"]
                    }
                }
            }
        ]
        
        self.system_prompt_template = """You are an Expert Cloud Economist and AWS Athena Analytics Copilot.
Your goal is to provide deep, actionable insights into query costs and performance.
Don't just list data—analyze it. Find the "Why".

You have access to the following SPECIFIC tools:

1. `fetch_athena_queries`: Get raw query data. Requires `start_date` and `end_date`.
2. `analyze_cost_increase`: Analyze cost spikes. Requires `baseline_start`, `baseline_end`, `spike_start`, `spike_end`.
3. `compare_expensive_queries`: Find expensive query patterns. Requires `start_date` and `end_date`.

GUIDELINES:
- Today is {current_date}.
- Use YYYY-MM-DD for dates.
- If data is empty, suggest valid date ranges or ask to try a wider range.
- **CRITICAL**: Only mention workgroups, tables, or drivers that appear in the tool output. Do NOT invent workgroup names like 'auditing' or 'payments'.

RESPONSE FORMAT (Use Markdown):
## 📊 Executive Summary
(1-2 sentences highlighting the most critical finding, e.g., "Costs spiked 40% due to unpartitioned queries in the 'reporting' workgroup.")

## 🔍 Key Discoveries
- **Driver 1:** (Detail about specific tables, workgroups, or users causing impact)
- **Driver 2:** (Secondary factors)

## 💡 Recommendations
1. **Immediate:** (e.g., "Add 'LIMIT' to ad-hoc queries")
2. **Long-term:** (e.g., "Partition table 'logs_events' by date")

## 💰 Estimated Impact
(e.g., "Implementing these could save ~$50/month")

EXAMPLES:
- User: "Why did costs go up last week?"
  -> Call `analyze_cost_increase`.
  -> Response: "Costs increased by $150 (30%) due to 5 huge queries scanning the entire 'logs' table..."

- User: "Show me expensive queries"
  -> Call `compare_expensive_queries`.
  -> Response: "The top query scanned 500GB ($2.50) alone. It selects * from a 10TB table..."
"""

    def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call an MCP tool and return the result."""
        try:
            print(f"Calling tool: {tool_name} with args: {arguments}")
            if tool_name == "fetch_athena_queries":
                result = fetch_athena_queries(
                    workgroup=arguments.get("workgroup"),
                    start_date=arguments["start_date"],
                    end_date=arguments["end_date"],
                    output_dir=None
                )
                return result
            elif tool_name == "analyze_cost_increase":
                result = analyze_cost_increase(
                    csv_file=None,
                    baseline_start=arguments["baseline_start"],
                    baseline_end=arguments["baseline_end"],
                    spike_start=arguments["spike_start"],
                    spike_end=arguments["spike_end"],
                    workgroup=arguments.get("workgroup")
                )
                return result
            elif tool_name == "compare_expensive_queries":
                result = compare_expensive_queries(
                    csv_file=None,
                    query_pattern=arguments.get("query_pattern"),
                    query_id=arguments.get("query_id"),
                    baseline_start=arguments.get("baseline_start"),
                    baseline_end=arguments.get("baseline_end"),
                    target_date=arguments.get("target_date"),
                    start_date=arguments["start_date"],
                    end_date=arguments["end_date"],
                    workgroup=arguments.get("workgroup")
                )
                return result
            else:
                return {"success": False, "error": f"Unknown tool: {tool_name}"}
        except Exception as e:
            traceback.print_exc()
            return {"success": False, "error": str(e)}

    def _format_tool_result(self, tool_name: str, result: Dict[str, Any]) -> str:
        """Format tool result into a readable string."""
        if not result.get("success"):
            return f"Error: {result.get('error', 'Unknown error')}"
        
        # Return a summary that the LLM can use to generate a natural response
        if tool_name == "fetch_athena_queries":
            return f"Successfully fetched {result.get('matched_count', 0)} queries. File saved to {result.get('file_path', 'N/A')}"
        elif tool_name == "analyze_cost_increase":
            period_comp = result.get('period_comparison', {})
            baseline = period_comp.get('baseline', {})
            spike = period_comp.get('spike', {})
            changes = period_comp.get('changes', {})
            
            response = f"Cost Analysis Results:\n"
            response += f"- Baseline period ({baseline.get('start_date', 'N/A')} to {baseline.get('end_date', 'N/A')}): {baseline.get('query_count', 0):,} queries, {baseline.get('total_gb', 0):.2f} GB\n"
            response += f"- Spike period ({spike.get('start_date', 'N/A')} to {spike.get('end_date', 'N/A')}): {spike.get('query_count', 0):,} queries, {spike.get('total_gb', 0):.2f} GB\n"
            if changes:
                response += f"- Data scanned increase: {changes.get('daily_data_scanned_pct', 0):.1f}%\n"
                response += f"- Query count change: {changes.get('query_count_pct', 0):.1f}%\n"
            
            top_queries = result.get('top_queries', {})
            if top_queries and isinstance(top_queries, dict):
                if 'spike' in top_queries and top_queries['spike']:
                    val = top_queries['spike'][0].get('gb', 0)
                    response += f"\nTop expensive query in spike period: {val:.2f} GB"

            # Add workgroup breakdown
            workgroup_comparison = result.get('workgroup_comparison', [])
            if workgroup_comparison:
                response += "\n\nWorkgroup Breakdown (Top Drivers):"
                for wg in workgroup_comparison[:3]: # Top 3 drivers
                    name = wg.get('workgroup', 'Unknown')
                    gb_change = wg.get('gb_change', 0)
                    spike_gb = wg.get('total_gb_spike', 0)
                    response += f"\n- {name}: +{gb_change:.2f} GB increase (Total Spike: {spike_gb:.2f} GB)"

            return response
        elif tool_name == "compare_expensive_queries":
            stats = result.get('statistics', {})
            query_details = result.get('query_details', [])
            response = f"Query Comparison Results:\n"
            response += f"- Total queries analyzed: {stats.get('total_queries', 0):,}\n"
            response += f"- Average data scanned: {stats.get('avg_data_scanned_gb', 0):.2f} GB\n"
            response += f"- Max data scanned: {stats.get('max_data_scanned_gb', 0):.2f} GB\n"
            if query_details:
                response += f"\nMost expensive query: {query_details[0].get('data_scanned_gb', 0):.2f} GB"
            return response
        
        return json.dumps(result, indent=2, default=str)

    async def chat(self, user_message: str, conversation_history: List[Dict[str, str]]) -> str:
        """Process a user message and return a response using OpenAI-compatible API."""
        
        # If client is not available, return simplified fallback
        if not self.client:
             # Basic fallback logic if no LLM is connected
             return "I'm sorry, I can't connect to the local AI model (Ollama). Please ensure it is running on http://localhost:11434."
        
        # Build messages for API
        current_date_str = datetime.now().strftime("%Y-%m-%d")
        system_prompt = self.system_prompt_template.format(current_date=current_date_str)
        messages = [{"role": "system", "content": system_prompt}]
        for msg in conversation_history[-10:]:  # Keep last 10 messages for context
            role = msg["role"]
            if role not in ["user", "assistant", "system", "tool"]: # Ensure valid roles
                 role = "user"
            messages.append({
                "role": role,
                "content": msg["content"]
            })
        messages.append({
            "role": "user",
            "content": user_message
        })
        
        try:
            print(f"Sending request to LLM: {self.model_name}")
            # First call to LLM
            response = self.client.chat.completions.create(
                model=self.model_name,
                messages=messages,
                tools=self.tools,
                tool_choice="auto"
            )
            
            response_message = response.choices[0].message
            tool_calls = response_message.tool_calls
            
            # Fallback for local LLMs: Check if content contains a JSON tool call
            if not tool_calls and response_message.content:
                import re
                # Llama 3 variable formats:
                # 1. {"name": "tool", "parameters": {...}}
                # 2. <tool_code>...
                
                # Check for standard JSON tool call pattern
                json_match = re.search(r'\{[\s\S]*"name"[\s\S]*"parameters"[\s\S]*\}', response_message.content)
                if json_match:
                    try:
                        tool_data = json.loads(json_match.group(0))
                        function_name = tool_data.get("name")
                        function_args = tool_data.get("parameters", {})
                        
                        if function_name:
                            print(f"Detected embedded tool call: {function_name}")
                            # Create a mock tool call object to reuse logic
                            messages.append(response_message)
                            
                            tool_result = self._call_tool(function_name, function_args)
                            formatted_result = self._format_tool_result(function_name, tool_result)
                            
                            messages.append({
                                "role": "tool", # Note: Local models might need 'user' role for tool results if they don't strictly follow OpenAI format, but 'tool' is standard
                                "name": function_name,
                                "content": formatted_result,
                                "tool_call_id": "call_fallback_" + datetime.now().strftime("%H%M%S") # Fake ID
                            })
                            
                            # Get final response
                            second_response = self.client.chat.completions.create(
                                model=self.model_name,
                                messages=messages
                            )
                            return second_response.choices[0].message.content
                    except Exception as e:
                        print(f"Failed to parse fallback tool call: {e}")
            
            
            if tool_calls:
                # LLM wants to use tools
                messages.append(response_message) # Add assistant's tool call request to history
                
                for tool_call in tool_calls:
                    function_name = tool_call.function.name
                    function_args = json.loads(tool_call.function.arguments)
                    
                    tool_result = self._call_tool(function_name, function_args)
                    formatted_result = self._format_tool_result(function_name, tool_result)
                    
                    messages.append({
                        "tool_call_id": tool_call.id,
                        "role": "tool",
                        "name": function_name,
                        "content": formatted_result,
                    })
                
                # Get final response from LLM
                second_response = self.client.chat.completions.create(
                    model=self.model_name,
                    messages=messages
                )
                return second_response.choices[0].message.content
            
            return response_message.content if response_message.content else "I've processed your request."
            
        except Exception as e:
            traceback.print_exc()
            return f"I encountered an error connecting to the AI model: {str(e)}. Please check if Ollama is running and the model '{self.model_name}' is downloaded."

