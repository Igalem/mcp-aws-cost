"""AI Agent for AWS Athena Analytics using Claude API and MCP tools."""

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
    import anthropic
except ImportError:
    anthropic = None

from src.tools.fetch_queries import fetch_athena_queries
from src.tools.analyze_cost import analyze_cost_increase
from src.tools.compare_queries import compare_expensive_queries


class AthenaAnalyticsAgent:
    """AI Agent that uses Claude API to interact with users and call MCP tools."""
    
    def __init__(self):
        self.client = None
        if anthropic:
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                self.client = anthropic.Anthropic(api_key=api_key)
        
        # Define available tools for Claude
        self.tools = [
            {
                "name": "fetch_athena_queries",
                "description": "Query Athena query execution data from PostgreSQL database and export to CSV. Use this when the user wants to fetch, export, or get query data.",
                "input_schema": {
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
                        }
                    },
                    "required": ["start_date", "end_date"]
                }
            },
            {
                "name": "analyze_cost_increase",
                "description": "Analyze cost increases by comparing baseline vs spike periods. Use this when the user asks about cost spikes, cost increases, or wants to compare two time periods.",
                "input_schema": {
                    "type": "object",
                    "properties": {
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
                            "description": "Optional workgroup filter"
                        }
                    },
                    "required": ["baseline_start", "baseline_end", "spike_start", "spike_end"]
                }
            },
            {
                "name": "compare_expensive_queries",
                "description": "Compare expensive queries and extract patterns. Use this when the user asks about expensive queries, query patterns, or wants to analyze query performance.",
                "input_schema": {
                    "type": "object",
                    "properties": {
                        "start_date": {
                            "type": "string",
                            "description": "Start date for query analysis (YYYY-MM-DD)",
                            "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
                        },
                        "end_date": {
                            "type": "string",
                            "description": "End date for query analysis (YYYY-MM-DD)",
                            "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
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
        ]
        
        self.system_prompt = """You are an AI assistant helping users analyze AWS Athena query costs and performance. 
You have access to tools that can:
1. Fetch query data from the database
2. Analyze cost increases between time periods
3. Compare expensive queries and find patterns

When interacting with users:
- Be conversational and helpful
- Ask clarifying questions if dates or parameters are unclear
- Use natural language to explain results
- Suggest follow-up analyses when relevant
- If dates are mentioned relatively (like "last week"), calculate the actual dates

Always use the appropriate tool when the user asks for data analysis. If the user is just asking a general question or needs clarification, respond conversationally without using tools."""

    def _parse_relative_date(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        """Parse relative dates like 'last 7 days', 'last month', etc."""
        text_lower = text.lower()
        now = datetime.now()
        
        if 'last 7 days' in text_lower or 'past week' in text_lower:
            return (now - timedelta(days=7)).strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')
        elif 'last 14 days' in text_lower or 'past 2 weeks' in text_lower:
            return (now - timedelta(days=14)).strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')
        elif 'last 30 days' in text_lower or 'past month' in text_lower or 'last month' in text_lower:
            return (now - timedelta(days=30)).strftime('%Y-%m-%d'), now.strftime('%Y-%m-%d')
        
        return None, None

    def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """Call an MCP tool and return the result."""
        try:
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
        """Format tool result into a readable string for Claude."""
        if not result.get("success"):
            return f"Error: {result.get('error', 'Unknown error')}"
        
        # Return a summary that Claude can use to generate a natural response
        if tool_name == "fetch_athena_queries":
            return f"Successfully fetched {result.get('matched_count', 0)} queries. File saved to {result.get('file_path', 'N/A')}"
        elif tool_name == "analyze_cost_increase":
            summary = result.get('summary', {})
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
                    response += f"\nTop expensive query in spike period: {top_queries['spike'][0].get('gb', 0):.2f} GB"
            
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
        """Process a user message and return a response using Claude API."""
        
        # If Claude API is not available, use intelligent fallback with MCP tools
        if not self.client:
            return self._fallback_response(user_message, conversation_history)
        
        # Build messages for Claude
        messages = []
        for msg in conversation_history[-10:]:  # Keep last 10 messages for context
            messages.append({
                "role": msg["role"],
                "content": msg["content"]
            })
        messages.append({
            "role": "user",
            "content": user_message
        })
        
        try:
            # Call Claude with tool definitions
            response = self.client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=4096,
                system=self.system_prompt,
                messages=messages,
                tools=self.tools
            )
            
            # Process the response
            final_response = ""
            tool_results = []
            
            for content_block in response.content:
                if content_block.type == "text":
                    final_response += content_block.text
                elif content_block.type == "tool_use":
                    # Claude wants to use a tool
                    tool_name = content_block.name
                    tool_input = content_block.input
                    
                    # Call the tool
                    tool_result = self._call_tool(tool_name, tool_input)
                    formatted_result = self._format_tool_result(tool_name, tool_result)
                    
                    tool_results.append({
                        "tool_use_id": content_block.id,
                        "content": formatted_result
                    })
            
            # If tools were called, send results back to Claude for final response
            if tool_results:
                messages.append({
                    "role": "assistant",
                    "content": response.content
                })
                
                # Add tool results
                messages.append({
                    "role": "user",
                    "content": [
                        {
                            "type": "tool_result",
                            "tool_use_id": tr["tool_use_id"],
                            "content": tr["content"]
                        }
                        for tr in tool_results
                    ]
                })
                
                # Get final response from Claude
                final_response_obj = self.client.messages.create(
                    model="claude-3-5-sonnet-20241022",
                    max_tokens=4096,
                    system=self.system_prompt,
                    messages=messages
                )
                
                # Extract text from final response
                final_response = ""
                for content_block in final_response_obj.content:
                    if content_block.type == "text":
                        final_response += content_block.text
            
            return final_response if final_response else "I've processed your request. How can I help you further?"
            
        except Exception as e:
            traceback.print_exc()
            return f"I encountered an error: {str(e)}. Let me try a simpler approach."

    def _fallback_response(self, user_message: str, conversation_history: List[Dict[str, str]] = None) -> str:
        """Fallback response when Claude API is not available - still uses MCP tools with conversation awareness."""
        if conversation_history is None:
            conversation_history = []
        
        message_lower = user_message.lower().strip()
        
        # Check conversation history for context
        recent_context = ' '.join([msg.get('content', '') for msg in conversation_history[-3:]])
        context_lower = recent_context.lower()
        
        # Check for simple greetings first - show shorter message
        is_greeting = (
            message_lower in ['hello', 'hi', 'hey', 'help'] or 
            message_lower.startswith('hello') or 
            message_lower.startswith('hi ') or
            message_lower.startswith('hey ') or
            ('can you help' in message_lower and len(message_lower) < 30)
        )
        
        if is_greeting and len(conversation_history) == 0:
            return """Hi! I'm here to help you analyze your AWS Athena queries and costs. 

I can help you understand:
- Query patterns and usage
- Cost trends and increases
- Expensive queries and optimization opportunities

What would you like to explore today? Feel free to ask me questions, and I'll help guide you through the analysis."""
        
        # Check for user confirmation (yes, analyze it, etc.) in response to previous question
        is_confirmation = any(phrase in message_lower for phrase in ['yes', 'yeah', 'yep', 'sure', 'ok', 'okay', 'go ahead', 'do it', 'analyze it', 'please'])
        if is_confirmation and len(conversation_history) > 0:
            # Check if previous assistant message was asking about cost analysis
            last_assistant_msg = ''
            for msg in reversed(conversation_history):
                if msg.get('role') == 'assistant':
                    last_assistant_msg = msg.get('content', '').lower()
                    break
            
            # If assistant asked a question and user confirmed, check if it's about cost analysis
            if last_assistant_msg and ('would you like' in last_assistant_msg or 'cost' in last_assistant_msg):
                # Check if context mentions "last week" for cost analysis
                if 'last week' in context_lower:
                    try:
                        now = datetime.now()
                        spike_end = now.strftime('%Y-%m-%d')
                        spike_start = (now - timedelta(days=7)).strftime('%Y-%m-%d')
                        baseline_end = spike_start
                        baseline_start = (now - timedelta(days=14)).strftime('%Y-%m-%d')
                        
                        from backend.main import format_tool_response
                        result = self._call_tool('analyze_cost_increase', {
                            'baseline_start': baseline_start,
                            'baseline_end': baseline_end,
                            'spike_start': spike_start,
                            'spike_end': spike_end
                        })
                        response = format_tool_response('analyze_cost_increase', result)
                        return f"Perfect! I've analyzed the cost increase comparing last week ({spike_start} to {spike_end}) to the week before ({baseline_start} to {baseline_end}).\n\n{response}\n\nWould you like me to investigate the specific queries causing this increase?"
                    except Exception as e:
                        traceback.print_exc()
                        return f"I encountered an error while analyzing: {str(e)}. Could you try again?"
        
        # Check for explicit action requests (fetch, analyze, compare, show me, get me)
        explicit_action = any(phrase in message_lower for phrase in [
            'fetch', 'get me', 'show me', 'export', 'download', 'create a report',
            'generate', 'run', 'execute', 'analyze', 'compare'
        ])
        
        # Check for cost/spike mentions - be conversational, don't immediately act
        cost_mentioned = any(word in message_lower for word in ['cost', 'spike', 'increase', 'went up', 'higher', 'expensive', 'spending'])
        if cost_mentioned and not explicit_action and not is_confirmation:
            # User is asking about costs - be conversational, ask questions
            if 'last week' in message_lower or 'last week' in context_lower:
                return "I can help investigate that cost increase from last week! To analyze it properly, I'll compare last week to the week before to see what changed.\n\nWould you like me to run that analysis? Just say \"yes\" or \"analyze it\" and I'll get started."
            elif 'why' in message_lower or 'what' in message_lower or 'how' in message_lower:
                return "I'd be happy to help you understand what's happening with your costs! To investigate, I'll need to compare two time periods:\n\n- What period should I use as the baseline (normal costs)?\n- What period had the cost increase?\n\nFor example, if costs went up last week, I can compare last week to the week before. Would you like me to analyze that?\n\nOr if you'd prefer, you can tell me specific dates like: \"Compare 2025-12-01 to 2025-12-07 (baseline) vs 2025-12-08 to 2025-12-14 (spike)\""
            else:
                return "I can help you investigate the cost increase! To do a proper analysis, I need to understand:\n\n- When did you notice the costs went up? (specific dates or relative like \"last week\")\n- What period should I compare it to? (the normal baseline period)\n\nOnce you provide those details, I can analyze what changed and identify the queries causing the increase. What time periods would you like me to compare?"
        
        # Check for explicit help requests
        explicit_help = any(phrase in message_lower for phrase in [
            'what can you do', 'capabilities', 'help me with', 'show me how', 
            'how do i', 'how can i', 'what tools', 'what are your', 'list tools'
        ])
        
        if explicit_help:
            return """🤖 **I can help you analyze AWS Athena queries using these tools:**

1. **Fetch Queries** - Export query data to CSV
   Example: "Fetch queries from last 7 days"

2. **Analyze Cost Increase** - Compare baseline vs spike periods
   Example: "Analyze cost increase from baseline 2025-12-01 to 2025-12-07 vs spike 2025-12-08 to 2025-12-14"

3. **Compare Expensive Queries** - Find and compare expensive query patterns
   Example: "Compare expensive queries from last 30 days"

💡 **Tips:**
- Specify dates like "last 7 days", "2025-12-01 to 2025-12-31", or "last month"
- Filter by workgroup: "workgroup: ETLs"
- Ask about cost spikes, query patterns, or data usage

**Note:** To enable full AI agent capabilities with natural language understanding, set the ANTHROPIC_API_KEY environment variable."""
        
        # Only call tools if user explicitly requests an action
        if explicit_action:
            try:
                # Import here to avoid circular imports
                import sys
                import os
                sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
                from backend.main import determine_tool_and_params, format_tool_response
                
                tool_info = determine_tool_and_params(user_message)
                tool_name = tool_info['tool']
                params = {k: v for k, v in tool_info['params'].items() if v is not None}
                
                # Only proceed if we have enough info, otherwise ask for clarification
                if tool_name == 'fetch_athena_queries':
                    if params.get('start_date') and params.get('end_date'):
                        result = self._call_tool(tool_name, params)
                        response = format_tool_response(tool_name, result)
                        return response + "\n\nIs there anything specific you'd like to analyze about these queries?"
                    else:
                        return "I'd be happy to fetch that data for you! Could you specify the date range? For example:\n- \"Fetch queries from last 7 days\"\n- \"Get queries from 2025-12-01 to 2025-12-31\""
                        
                elif tool_name == 'analyze_cost_increase':
                    # Check if user confirmed or provided dates
                    if 'yes' in message_lower or 'analyze' in message_lower or 'do it' in message_lower or 'please' in message_lower:
                        # User confirmed - check if we have dates from context
                        if 'last week' in message_lower or 'last week' in context_lower:
                            now = datetime.now()
                            spike_end = now.strftime('%Y-%m-%d')
                            spike_start = (now - timedelta(days=7)).strftime('%Y-%m-%d')
                            baseline_end = spike_start
                            baseline_start = (now - timedelta(days=14)).strftime('%Y-%m-%d')
                            params.update({
                                'baseline_start': baseline_start,
                                'baseline_end': baseline_end,
                                'spike_start': spike_start,
                                'spike_end': spike_end
                            })
                    
                    if all(params.get(k) for k in ['baseline_start', 'baseline_end', 'spike_start', 'spike_end']):
                        result = self._call_tool(tool_name, params)
                        response = format_tool_response(tool_name, result)
                        return response + "\n\nWould you like me to investigate the specific queries causing this increase?"
                    else:
                        return "I can help analyze that cost increase! To do a proper comparison, I need:\n- Baseline period dates (normal period)\n- Spike period dates (when costs increased)\n\nFor example: \"Analyze cost increase: baseline from 2025-12-01 to 2025-12-07, spike from 2025-12-08 to 2025-12-14\""
                        
                elif tool_name == 'compare_expensive_queries':
                    if params.get('start_date') and params.get('end_date'):
                        result = self._call_tool(tool_name, params)
                        response = format_tool_response(tool_name, result)
                        return response + "\n\nWould you like me to analyze the patterns in these expensive queries?"
                    else:
                        return "I'd be happy to compare expensive queries! Could you specify the date range? For example:\n- \"Compare expensive queries from last 30 days\"\n- \"Show expensive queries from 2025-12-01 to 2025-12-31\""
            except Exception as e:
                traceback.print_exc()
                pass  # Fall through to conversational response
        
        # Conversational responses for queries about queries
        query_mentioned = any(word in message_lower for word in ['query', 'queries', 'execution', 'performance'])
        if query_mentioned and not explicit_action:
            if 'expensive' in message_lower or 'most' in message_lower or 'top' in message_lower:
                return "I can help you find the expensive queries! To do that, I'll need to know:\n\n- What time period should I look at? (e.g., \"last 30 days\", \"December 2025\")\n- Are you interested in a specific workgroup, or all workgroups?\n\nOnce you tell me the date range, I can analyze and show you which queries are using the most data and costing the most. What period would you like me to examine?"
            else:
                return "I can help you understand your query patterns! What specifically would you like to know?\n\n- Query volume trends?\n- Data scanning patterns?\n- Performance issues?\n- Cost optimization opportunities?\n\nTell me what you're curious about, and I'll help guide you through the analysis!"
        
        # Default conversational response
        return "I'm here to help you understand your AWS Athena queries and costs! What would you like to explore?\n\nYou can ask me about:\n- Cost trends and increases\n- Query patterns and usage\n- Expensive queries\n- Optimization opportunities\n\nJust tell me what you're curious about, and I'll ask clarifying questions to help you get the insights you need!"

