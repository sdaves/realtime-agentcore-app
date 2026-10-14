"""
Real-Time RAG Agent — Deployed to AgentCore Runtime
=====================================================
Uses Strands Agents SDK with the BedrockAgentCoreApp wrapper.
Connects to AgentCore Gateway to access the vector search MCP tool.

Deploy with:
  agentcore configure -e agent.py --disable-memory
  agentcore deploy
  agentcore invoke '{"prompt": "What is the status of ORDER-12345?"}'
"""

import os
from bedrock_agentcore.runtime import BedrockAgentCoreApp
from strands import Agent
from strands.models import BedrockModel

app = BedrockAgentCoreApp()

SYSTEM_PROMPT = """You are a real-time information assistant powered by a live streaming data pipeline.

Your knowledge base is updated in near real-time — typically within 15 seconds of an event occurring.
When you retrieve context, it includes timestamps showing exactly when each piece of data was indexed.

INSTRUCTIONS:
1. Always use the vector search tool when answering questions about current status or recent events.
2. Include the timestamp from retrieved context in your response, e.g.:
   "As of 12 seconds ago, the order status is 'shipped'."
3. If retrieved context is older than expected, say so transparently.
4. Never guess — if vector search returns no results, say you don't have current data.
5. Be concise. Lead with the answer, then supporting detail."""


@app.entrypoint
def invoke(payload):
    """AgentCore Runtime entry point — called when the agent is invoked."""
    prompt = payload.get("prompt", "Hello!")

    model = BedrockModel(
        model_id="us.anthropic.claude-3-haiku-20240307-v1:0",
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
    )

    agent = Agent(
        model=model,
        system_prompt=SYSTEM_PROMPT,
        # Tools are automatically discovered from the AgentCore Gateway
        # MCP endpoint configured during `agentcore configure`
    )

    result = agent(prompt)
    return {"result": str(result.message)}


if __name__ == "__main__":
    app.run()