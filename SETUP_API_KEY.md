# Setting Up Anthropic API Key

## Step 1: Get Your Full API Key

The JSON you provided shows a partial key hint: `sk-ant-api03-R2D...igAA`

You need to get the **full API key** from your Anthropic console:

1. Go to https://console.anthropic.com/
2. Navigate to API Keys section
3. Find your "Developer Key" (created on 2024-10-30)
4. Click to reveal the full key (it starts with `sk-ant-api03-`)

## Step 2: Add to .env File

Once you have the full key, add it to your `.env` file:

```bash
# Edit the .env file
nano .env
# or
code .env
```

Add this line (replace with your actual full key):
```
ANTHROPIC_API_KEY=sk-ant-api03-YOUR_FULL_KEY_HERE
```

**Important:** 
- Never commit the `.env` file to git (it should be in `.gitignore`)
- Never share your API key publicly
- The key should start with `sk-ant-api03-` and be much longer than the partial hint

## Step 3: Restart the Backend

After adding the key, restart your backend server:

```bash
# Stop the current backend (Ctrl+C or kill the process)
lsof -ti:8000 | xargs kill

# Start it again
cd /Users/igal.emona/mcp-aws-cost
source venv/bin/activate
python -m backend.main
```

## Step 4: Test It

Try asking the AI agent a question in the chat:

```
"Help me analyze cost increases from last week"
```

If the agent responds intelligently and asks clarifying questions, it's working!

## Troubleshooting

### Agent still shows fallback message
- Check that the key is in `.env` file
- Verify the key starts with `sk-ant-api03-`
- Make sure you restarted the backend after adding the key
- Check backend logs for errors

### API Key Error
- Verify the key is correct (copy-paste carefully)
- Check that the key hasn't been revoked in Anthropic console
- Ensure you have credits/quota available

### Test the Key Directly
```bash
source venv/bin/activate
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('Key found:', bool(os.getenv('ANTHROPIC_API_KEY'))); print('Key starts with:', os.getenv('ANTHROPIC_API_KEY', '')[:15] + '...' if os.getenv('ANTHROPIC_API_KEY') else 'None')"
```




