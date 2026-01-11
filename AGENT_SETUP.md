# AI Agent Setup Guide

The "Ask AI" chat interface now uses an intelligent agent powered by Claude API that can:
- Understand natural language queries
- Automatically decide which MCP tools to use
- Have conversational interactions with users
- Maintain context across multiple messages
- Ask clarifying questions when needed

## Setup

### 1. Install Dependencies

```bash
source venv/bin/activate
pip install -r requirements.txt
```

This will install the `anthropic` package needed for Claude API.

### 2. Get Anthropic API Key

1. Sign up at https://console.anthropic.com/
2. Create an API key
3. Set it as an environment variable:

```bash
export ANTHROPIC_API_KEY="your-api-key-here"
```

Or add it to your `.env` file:
```
ANTHROPIC_API_KEY=your-api-key-here
```

### 3. Restart the Backend

```bash
python -m backend.main
```

## How It Works

### With Claude API (Full Agent Mode)

When `ANTHROPIC_API_KEY` is set, the agent:
- Uses Claude 3.5 Sonnet to understand user intent
- Automatically selects and calls appropriate MCP tools
- Formats tool results into natural language responses
- Maintains conversation context
- Can ask follow-up questions

**Example interactions:**

User: "I noticed our costs went up last week. Can you analyze that?"
Agent: "I'd be happy to help analyze the cost increase! To compare properly, I need to know:
- What date range should I use as the baseline (normal period)?
- What date range should I use as the spike period?

For example, if last week was Dec 8-14, I could compare it to the week before (Dec 1-7). Would that work?"

User: "Yes, compare Dec 1-7 to Dec 8-14"
Agent: *Calls analyze_cost_increase tool* "I've analyzed the cost increase between those periods. Here's what I found..."

### Without Claude API (Fallback Mode)

If `ANTHROPIC_API_KEY` is not set, the agent falls back to:
- Simple keyword matching
- Basic tool selection
- Limited conversational capabilities

## Features

### Intelligent Tool Selection

The agent automatically chooses the right tool based on user intent:
- **fetch_athena_queries**: When user wants to fetch, export, or get query data
- **analyze_cost_increase**: When user asks about cost spikes or wants to compare periods
- **compare_expensive_queries**: When user asks about expensive queries or patterns

### Natural Language Understanding

The agent understands:
- Relative dates: "last week", "last month", "past 7 days"
- Absolute dates: "2025-12-01 to 2025-12-31"
- Workgroup filters: "workgroup: ETLs"
- Conversational queries: "What's causing our costs to spike?"

### Context Awareness

The agent remembers:
- Previous messages in the conversation
- Tool results from earlier in the conversation
- User preferences and clarifications

## Example Conversations

### Example 1: Cost Analysis

**User:** "Our Athena costs spiked last week. What happened?"

**Agent:** "I can help analyze that cost spike! To do a proper comparison, I'll need to compare last week to a baseline period. 

Last week would be December 24-31. Should I compare it to the week before (December 17-23), or would you prefer a different baseline period?"

**User:** "Compare to the week before"

**Agent:** *Calls analyze_cost_increase tool*
"I've analyzed the cost increase between December 17-23 (baseline) and December 24-31 (spike). Here are the key findings:

- **Cost Increase**: Data scanned increased by 43.0% (from 142,570 GB to 203,931 GB)
- **Query Volume**: Query count increased by 185.8% (from 3,582 to 10,237 queries)
- **Top Expensive Queries**: The spike period shows several queries scanning over 2.5 TB each, all with the UNLOAD pattern

The significant increase in both query volume and data scanned suggests either:
1. New workloads were introduced
2. Existing queries started scanning more data
3. Query patterns changed

Would you like me to dive deeper into the specific query patterns causing this increase?"

### Example 2: Query Investigation

**User:** "Show me the most expensive queries from last month"

**Agent:** *Calls compare_expensive_queries tool*
"I've analyzed the expensive queries from the last 30 days. Here's what I found:

- **Total Queries Analyzed**: 16,690 queries
- **Average Data Scanned**: 32.86 GB per query
- **Most Expensive Query**: Scanned 2,577.58 GB on December 29

The top 5 most expensive queries all scanned over 2.5 TB each and occurred between December 25-29. They all share the UNLOAD pattern, which suggests they're exporting large datasets.

Would you like me to:
1. Analyze the patterns in these expensive queries?
2. Compare them to queries from an earlier period?
3. Look at a specific workgroup?"

## Troubleshooting

### Agent not responding intelligently

- Check that `ANTHROPIC_API_KEY` is set correctly
- Verify the API key is valid and has credits
- Check backend logs for errors

### Tool calls failing

- Ensure PostgreSQL database is accessible
- Check that date ranges are valid
- Verify workgroup names exist

### Responses are too verbose

The agent is designed to be conversational. If you want more concise responses, you can ask: "Give me a brief summary" or "Just the numbers please"

## Cost Considerations

Claude API usage is charged per token. Typical costs:
- Input: ~$3 per million tokens
- Output: ~$15 per million tokens

A typical conversation with tool calls might use:
- 1,000-5,000 input tokens
- 500-2,000 output tokens

This translates to roughly $0.01-0.05 per conversation turn.




