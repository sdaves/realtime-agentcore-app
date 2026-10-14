"""
Vector Search Lambda — Bridge between AgentCore Gateway and OpenSearch
=======================================================================
This Lambda wraps OpenSearch k-NN vector search as an MCP-compatible tool.
AgentCore Gateway invokes this Lambda when an agent calls the vector search tool.

Input (from Gateway):
{
    "query": "What is the status of order 12345?",
    "top_k": 5
}

Output (to agent):
{
    "results": [
        {
            "text": "Event type: order_update. Status: shipped...",
            "score": 0.95,
            "timestamp": "2026-03-15T10:30:00Z",
            "entity_id": "ORDER-12345",
            "event_type": "order_update"
        }
    ],
    "count": 1,
    "query_embedding_ms": 48,
    "search_ms": 2
}
"""

import json
import os
import time
import logging

import boto3
from botocore.config import Config as BotoConfig
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Configuration from environment
OPENSEARCH_ENDPOINT = os.environ["OPENSEARCH_ENDPOINT"]
INDEX_NAME = os.environ["INDEX_NAME"]
EMBEDDING_MODEL_ID = os.environ["EMBEDDING_MODEL_ID"]
REGION = os.environ.get("AWS_REGION_NAME", os.environ.get("AWS_REGION", "us-east-1"))

# Bedrock client
bedrock = boto3.client(
    "bedrock-runtime",
    config=BotoConfig(region_name=REGION, retries={"max_attempts": 3, "mode": "adaptive"}),
)

# OpenSearch client with SigV4
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    REGION,
    "aoss",
    session_token=credentials.token,
)

host = OPENSEARCH_ENDPOINT.replace("https://", "")
os_client = OpenSearch(
    hosts=[{"host": host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=15,
)


def generate_query_embedding(text: str) -> list:
    """Generate embedding for the search query using Titan V2."""
    response = bedrock.invoke_model(
        modelId=EMBEDDING_MODEL_ID,
        contentType="application/json",
        accept="application/json",
        body=json.dumps({
            "inputText": text[:8000],
            "dimensions": 1024,
            "normalize": True,
        }),
    )
    result = json.loads(response["body"].read())
    return result["embedding"]


def search_vectors(embedding: list, top_k: int = 5) -> list:
    """Run k-NN search against OpenSearch."""
    query = {
        "size": top_k,
        "query": {
            "knn": {
                "embedding": {
                    "vector": embedding,
                    "k": top_k,
                }
            }
        },
        "_source": ["text", "entity_id", "timestamp", "event_type", "metadata"],
    }

    response = os_client.search(index=INDEX_NAME, body=query)

    results = []
    for hit in response["hits"]["hits"]:
        results.append({
            "text": hit["_source"].get("text", ""),
            "score": round(hit["_score"], 4),
            "timestamp": hit["_source"].get("timestamp", ""),
            "entity_id": hit["_source"].get("entity_id", hit["_id"]),
            "event_type": hit["_source"].get("event_type", ""),
            "metadata": hit["_source"].get("metadata", {}),
        })

    return results


def handler(event, context):
    """Lambda handler — called by AgentCore Gateway."""
    try:
        # Parse input
        body = event if isinstance(event, dict) else json.loads(event.get("body", "{}"))
        query = body.get("query", "")
        top_k = min(body.get("top_k", 5), 20)

        if not query:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "Missing 'query' parameter"}),
            }

        # Step 1: Generate query embedding
        embed_start = time.time()
        query_embedding = generate_query_embedding(query)
        embed_ms = round((time.time() - embed_start) * 1000)

        # Step 2: Search OpenSearch
        search_start = time.time()
        results = search_vectors(query_embedding, top_k)
        search_ms = round((time.time() - search_start) * 1000)

        logger.info(f"Query: '{query[:80]}' | {len(results)} results | embed={embed_ms}ms search={search_ms}ms")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "results": results,
                "count": len(results),
                "query_embedding_ms": embed_ms,
                "search_ms": search_ms,
            }),
        }

    except Exception as e:
        logger.error(f"Vector search failed: {e}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
        }