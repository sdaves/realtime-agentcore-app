"""
Glue Streaming Job — Real-Time RAG Pipeline
=============================================
Reads from Kinesis, transforms payloads, generates Titan embeddings,
and upserts vectors into OpenSearch Serverless.

Glue 4.0, gluestreaming engine.

Expected Kinesis record JSON:
{
    "event_id": "evt-abc123",
    "entity_id": "ORDER-12345",
    "event_type": "order_update",
    "timestamp": "2026-03-12T10:30:00Z",
    "data": {
        "status": "shipped",
        "description": "Order shipped via express delivery",
        "metadata": {"carrier": "DHL", "tracking": "DHL123456"}
    }
}
"""

import sys
import json
import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.config import Config as BotoConfig

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import (
    StructType, StructField, StringType, MapType,
)

# =============================================================================
# CONFIGURATION
# =============================================================================
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "kinesis_stream_name",
    "opensearch_endpoint",
    "opensearch_index",
    "embedding_model_id",
    "aws_region",
    "window_size",
    "TempDir",
])

STREAM_NAME = args["kinesis_stream_name"]
OPENSEARCH_ENDPOINT = args["opensearch_endpoint"]
INDEX_NAME = args["opensearch_index"]
EMBEDDING_MODEL_ID = args["embedding_model_id"]
AWS_REGION = args["aws_region"]
WINDOW_SIZE = args["window_size"]

logger = logging.getLogger("realtime-rag")
logger.setLevel(logging.INFO)

# Bedrock client
bedrock_client = boto3.client(
    "bedrock-runtime",
    config=BotoConfig(
        region_name=AWS_REGION,
        retries={"max_attempts": 3, "mode": "adaptive"},
    ),
)

# OpenSearch client with SigV4
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth

session = boto3.Session()
credentials = session.get_credentials().get_frozen_credentials()
awsauth = AWS4Auth(
    credentials.access_key,
    credentials.secret_key,
    AWS_REGION,
    "aoss",
    session_token=credentials.token,
)

os_host = OPENSEARCH_ENDPOINT.replace("https://", "")
os_client = OpenSearch(
    hosts=[{"host": os_host, "port": 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
    timeout=30,
)


# =============================================================================
# EMBEDDING
# =============================================================================
def generate_embedding(text):
    """Generate a 1024-dim embedding using Titan V2."""
    try:
        response = bedrock_client.invoke_model(
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
    except Exception as e:
        logger.error(f"Embedding failed: {e}")
        return None


def batch_generate_embeddings(texts, max_workers=10):
    """Generate embeddings in parallel using ThreadPoolExecutor."""
    embeddings = [None] * len(texts)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(generate_embedding, text): idx
            for idx, text in enumerate(texts)
        }
        for future in as_completed(future_to_idx):
            idx = future_to_idx[future]
            try:
                embeddings[idx] = future.result()
            except Exception as e:
                logger.error(f"Embedding thread {idx} failed: {e}")
    return embeddings


# =============================================================================
# OPENSEARCH INDEX + UPSERT
# =============================================================================
_index_created = False


def ensure_index_exists():
    """Create the k-NN vector index if it doesn't exist."""
    global _index_created
    if _index_created:
        return
    try:
        if not os_client.indices.exists(index=INDEX_NAME):
            os_client.indices.create(
                index=INDEX_NAME,
                body={
                    "settings": {
                        "index": {
                            "knn": True,
                            "knn.algo_param.ef_search": 512,
                        }
                    },
                    "mappings": {
                        "properties": {
                            "embedding": {
                                "type": "knn_vector",
                                "dimension": 1024,
                                "method": {
                                    "engine": "faiss",
                                    "name": "hnsw",
                                    "space_type": "cosinesimil",
                                    "parameters": {"ef_construction": 512, "m": 16},
                                },
                            },
                            "text": {"type": "text"},
                            "entity_id": {"type": "keyword"},
                            "timestamp": {"type": "keyword"},
                            "event_type": {"type": "keyword"},
                            "metadata": {"type": "object", "enabled": True},
                        }
                    },
                },
            )
            logger.info(f"Created index: {INDEX_NAME}")
        _index_created = True
    except Exception as e:
        logger.error(f"Index creation error: {e}")
        _index_created = True  # Don't block every batch on retry


def upsert_to_opensearch(documents):
    """
    Bulk index documents into OpenSearch Serverless.

    IMPORTANT: Vector search collections in OpenSearch Serverless do NOT
    support custom document IDs or upsert operations. We index without
    specifying _id and let OpenSearch auto-generate IDs.

    Deduplication is handled at query time — the vector search Lambda
    returns results sorted by score, and we include entity_id + timestamp
    as fields so the consumer can pick the freshest result per entity.
    """
    if not documents:
        return

    bulk_body = []
    for doc in documents:
        if doc.get("embedding") is None:
            continue

        bulk_body.append({"index": {"_index": INDEX_NAME}})
        bulk_body.append({
            "embedding": doc["embedding"],
            "text": doc["text"],
            "entity_id": doc.get("entity_id", ""),
            "metadata": doc.get("metadata", {}),
            "timestamp": doc["timestamp"],
            "event_type": doc.get("event_type", "unknown"),
        })

    if bulk_body:
        try:
            response = os_client.bulk(body=bulk_body)
            errors = [
                item for item in response.get("items", [])
                if item.get("index", {}).get("error")
            ]
            if errors:
                logger.error(f"Bulk index errors: {errors[:3]}")
            else:
                logger.info(f"Indexed {len(documents)} docs to {INDEX_NAME}")
        except Exception as e:
            logger.error(f"Bulk index failed: {e}")


# =============================================================================
# MICRO-BATCH PROCESSING
# =============================================================================
def processBatch(data_frame, batchId):
    """
    Process a single micro-batch from Kinesis.

    IMPORTANT: With inferSchema=true and classification=json, Glue
    auto-parses the Kinesis JSON payload. The DataFrame passed to
    this function contains the inferred schema column, NOT a raw
    'data' column. We convert to DynamicFrame to reliably access
    the nested fields.
    """
    if data_frame.count() == 0:
        return

    batch_start = time.time()
    record_count = data_frame.count()
    logger.info(f"Batch {batchId}: Processing {record_count} records")

    # Ensure OpenSearch index exists
    ensure_index_exists()

    # Convert to DynamicFrame for reliable field access
    dynamic_frame = DynamicFrame.fromDF(data_frame, glueContext, "from_kinesis")

    # Resolve any choice types that Glue inference may create
    resolved = dynamic_frame.resolveChoice(specs=[
        ("data.metadata", "cast:string")
    ])

    # Convert back to DataFrame and collect
    # Print schema on first batch for debugging
    if batchId == 0:
        resolved.toDF().printSchema()

    rows = resolved.toDF().collect()

    documents = []
    texts_to_embed = []

    for row in rows:
        try:
            # Access fields — Glue infers these from the JSON structure
            row_dict = row.asDict(recursive=True)

            event_id = row_dict.get("event_id", "")
            entity_id = row_dict.get("entity_id", "")
            event_type = row_dict.get("event_type", "")
            timestamp = row_dict.get("timestamp", "")
            data = row_dict.get("data", {})

            if not entity_id or not timestamp:
                logger.warning(f"Skipping record missing entity_id or timestamp: {row_dict}")
                continue

            # Build text for embedding
            parts = [f"Event type: {event_type}"]

            if isinstance(data, dict):
                status = data.get("status", "")
                description = data.get("description", "")
                metadata = data.get("metadata", {})

                if status:
                    parts.append(f"Status: {status}")
                if description:
                    parts.append(f"Description: {description}")
                if metadata and isinstance(metadata, dict):
                    meta_str = ", ".join(f"{k}: {v}" for k, v in metadata.items())
                    parts.append(f"Metadata: {meta_str}")
            elif isinstance(data, str):
                # metadata was cast to string by resolveChoice
                parts.append(f"Data: {data}")

            text = ". ".join(parts)
            texts_to_embed.append(text)

            # Parse metadata safely
            meta_dict = {}
            if isinstance(data, dict):
                raw_meta = data.get("metadata", {})
                if isinstance(raw_meta, dict):
                    meta_dict = raw_meta
                elif isinstance(raw_meta, str):
                    try:
                        meta_dict = json.loads(raw_meta)
                    except Exception:
                        meta_dict = {"raw": raw_meta}

            # OpenSearch Serverless VECTORSEARCH collections don't allow
            # hyphens in document IDs — replace with underscores
            safe_entity_id = entity_id.replace("-", "_")

            documents.append({
                "entity_id": safe_entity_id,
                "event_type": event_type,
                "timestamp": timestamp,
                "text": text,
                "metadata": meta_dict,
                "embedding": None,
            })

        except Exception as e:
            logger.error(f"Error processing row: {e}")
            continue

    if not documents:
        logger.info(f"Batch {batchId}: No valid documents to process")
        return

    # Generate embeddings in parallel
    embeddings = batch_generate_embeddings(texts_to_embed, max_workers=10)
    for i, emb in enumerate(embeddings):
        documents[i]["embedding"] = emb

    # Deduplicate within batch — keep latest timestamp per entity_id
    seen = {}
    for doc in documents:
        eid = doc["entity_id"]
        if eid not in seen or doc["timestamp"] > seen[eid]["timestamp"]:
            seen[eid] = doc
    deduped = list(seen.values())

    # Upsert to OpenSearch
    upsert_to_opensearch(deduped)

    batch_duration = time.time() - batch_start
    logger.info(
        f"Batch {batchId}: Processed {len(deduped)} docs in {batch_duration:.2f}s"
    )


# =============================================================================
# MAIN
# =============================================================================
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Get account ID for stream ARN
account_id = boto3.client("sts").get_caller_identity()["Account"]

# Create streaming DataFrame from Kinesis
kinesis_options = {
    "streamARN": f"arn:aws:kinesis:{AWS_REGION}:{account_id}:stream/{STREAM_NAME}",
    "startingPosition": "TRIM_HORIZON",
    "inferSchema": "true",
    "classification": "json",
}

data_frame_kinesis = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options=kinesis_options,
)

# Process micro-batches
glueContext.forEachBatch(
    frame=data_frame_kinesis,
    batch_function=processBatch,
    options={
        "windowSize": WINDOW_SIZE,
        "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/",
    },
)

job.commit()