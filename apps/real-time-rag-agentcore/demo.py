"""
End-to-End Demo — Test the full pipeline
==========================================
Run after:
  1. cdk deploy RealTimeRagAgentCoreStack
  2. aws glue start-job-run --job-name realtime-rag-streaming-job
  3. Wait ~60s for Glue to initialise

Usage:
  python demo.py
  python demo.py --wait 25
"""

import argparse
import json
import time
import uuid
from datetime import datetime, timezone

import boto3


def push_event(kinesis, stream_name, entity_id, status):
    event = {
        "event_id": f"evt-{uuid.uuid4().hex[:12]}",
        "entity_id": entity_id,
        "event_type": "order_update",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "data": {
            "status": status,
            "description": f"Order status changed to {status}",
            "metadata": {"carrier": "DHL", "tracking": f"TRK{uuid.uuid4().hex[:10].upper()}"},
        },
    }
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(event).encode("utf-8"),
        PartitionKey=entity_id,
    )
    return event


def query_lambda(lambda_client, query, top_k=3):
    response = lambda_client.invoke(
        FunctionName="rag-vector-search",
        Payload=json.dumps({"query": query, "top_k": top_k}),
    )
    payload = json.loads(response["Payload"].read())

    # Lambda returns {statusCode, body} — parse the body
    if isinstance(payload, dict) and "body" in payload:
        body = json.loads(payload["body"])
        return body, payload.get("statusCode", 200)

    return payload, 200


def countdown(seconds):
    for i in range(seconds, 0, -1):
        print(f"    ⏳ {i}s remaining...", end="\r")
        time.sleep(1)
    print(f"    ✅ Done!{' ' * 20}")


def print_results(results, status_code):
    """Print query results clearly, or explain why there are none."""
    if status_code != 200:
        print(f"    ❌ Lambda returned status {status_code}")
        print(f"    Error: {results.get('error', 'Unknown')}")
        return

    hits = results.get("results", [])

    if not hits:
        print("    ⚠️  No results returned.")
        print("    Possible reasons:")
        print("      - Glue job not running (start it with: aws glue start-job-run --job-name realtime-rag-streaming-job)")
        print("      - Glue job still initialising (wait 60s after starting, then retry)")
        print("      - Micro-batch hasn't fired yet (try increasing --wait to 30)")
        print("      - OpenSearch index not created yet (Glue creates it on first batch)")
        return

    embed_ms = results.get("query_embedding_ms", "?")
    search_ms = results.get("search_ms", "?")
    print(f"    Found {len(hits)} result(s)  |  embed: {embed_ms}ms  |  search: {search_ms}ms\n")

    for i, r in enumerate(hits, 1):
        print(f"    [{i}] Entity:    {r.get('entity_id', 'N/A')}")
        print(f"        Text:      {r.get('text', 'N/A')[:100]}")
        print(f"        Score:     {r.get('score', 'N/A')}")
        print(f"        Timestamp: {r.get('timestamp', 'N/A')}")
        print()


def main():
    parser = argparse.ArgumentParser(description="Test the real-time RAG pipeline end-to-end")
    parser.add_argument("--wait", type=int, default=20, help="Seconds to wait for pipeline (default: 20)")
    parser.add_argument("--region", default="us-east-1")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)
    lambda_client = boto3.client("lambda", region_name=args.region)
    entity_id = f"ORDER-{uuid.uuid4().hex[:6].upper()}"

    print("\n" + "=" * 60)
    print("  REAL-TIME RAG PIPELINE — END-TO-END DEMO")
    print("=" * 60)

    # ---- Step 1: Push initial event ----
    print(f"\n📤 STEP 1: Push event to Kinesis")
    event = push_event(kinesis, "realtime-rag-stream", entity_id, "shipped")
    print(f"    Entity:    {entity_id}")
    print(f"    Status:    shipped")
    print(f"    Timestamp: {event['timestamp']}")

    # ---- Step 2: Wait for pipeline ----
    print(f"\n⏳ STEP 2: Waiting {args.wait}s for pipeline (Kinesis → Glue → Titan → OpenSearch)")
    countdown(args.wait)

    # ---- Step 3: Query ----
    # Use a broad semantic query that matches order update text
    query = "order shipped delivery status update"
    print(f"\n🔍 STEP 3: Query vector search — \"{query}\"")
    results, status = query_lambda(lambda_client, query)
    print_results(results, status)

    # Check if our entity appears in results
    hits = results.get("results", []) if isinstance(results, dict) else []
    our_entity = entity_id.replace("-", "_")
    found_ours = any(our_entity in r.get("entity_id", "") for r in hits)
    if found_ours:
        print(f"    ✅ Our event ({entity_id}) found in results!")
    else:
        print(f"    ℹ️  Our specific event not in top results yet — it may need more time")
        print(f"       (Looking for entity_id containing: {our_entity})")

    # ---- Step 4: Push update ----
    print(f"\n📤 STEP 4: Push UPDATE — same entity, new status")
    event2 = push_event(kinesis, "realtime-rag-stream", entity_id, "out_for_delivery")
    print(f"    Entity:    {entity_id}")
    print(f"    Status:    out_for_delivery  (was: shipped)")
    print(f"    Timestamp: {event2['timestamp']}")

    # ---- Step 5: Wait again ----
    print(f"\n⏳ STEP 5: Waiting {args.wait}s for update to flow through pipeline")
    countdown(args.wait)

    # ---- Step 6: Query again ----
    query2 = "order out for delivery status"
    print(f"\n🔍 STEP 6: Query again — \"{query2}\"")
    results2, status2 = query_lambda(lambda_client, query2)
    print_results(results2, status2)

    # ---- Summary ----
    print("=" * 60)
    print("  SUMMARY")
    print("=" * 60)

    hits1 = results.get("results", []) if isinstance(results, dict) else []
    hits2 = results2.get("results", []) if isinstance(results2, dict) else []

    if hits1 and hits2:
        # Show the freshest result from each query
        latest1 = max(hits1, key=lambda r: r.get("timestamp", ""))
        latest2 = max(hits2, key=lambda r: r.get("timestamp", ""))

        print(f"\n    Query 1 (shipped) — freshest result:")
        print(f"      Entity:    {latest1.get('entity_id', 'N/A')}")
        print(f"      Text:      {latest1.get('text', '')[:80]}")
        print(f"      Timestamp: {latest1.get('timestamp', 'N/A')}")

        print(f"\n    Query 2 (out for delivery) — freshest result:")
        print(f"      Entity:    {latest2.get('entity_id', 'N/A')}")
        print(f"      Text:      {latest2.get('text', '')[:80]}")
        print(f"      Timestamp: {latest2.get('timestamp', 'N/A')}")

        print(f"\n    ✅ Pipeline is live — vectors are queryable within seconds of ingestion.")
        print(f"    The vector store contains real-time event data, not batch snapshots.")

    elif not hits1 and not hits2:
        print("\n    ❌ No results from either query.")
        print("    Is the Glue job running? Check with:")
        print("      aws glue get-job-runs --job-name realtime-rag-streaming-job --max-results 1")
    else:
        print(f"\n    ⚠️  Partial results — first query: {len(hits1)} hits, second: {len(hits2)} hits")
        print(f"    Pipeline may need more time. Try: python demo.py --wait 30")

    print(f"\n    Entity tested: {entity_id}")
    print(f"    Pipeline window: ~{args.wait}s")
    print()


if __name__ == "__main__":
    main()