"""
Sample Event Generator — Push test events to Kinesis
=====================================================
Usage:
  python generate_events.py --count 50 --stream realtime-rag-stream
  python generate_events.py --type order_update --entity-id ORDER-12345 --status shipped --count 1
"""

import argparse
import json
import random
import string
import uuid
from datetime import UTC, datetime

import boto3

# Sample data for generating realistic events
ORDER_STATUSES = ["received", "processing", "packed", "shipped", "out_for_delivery", "delivered"]
CARRIERS = ["DHL", "FedEx", "UPS", "Royal Mail", "Aramex"]
EVENT_TYPES = ["order_update", "inventory_change", "customer_action", "system_alert"]

DESCRIPTIONS = {
    "order_update": [
        "Order status changed by fulfilment system",
        "Shipping label generated and package dispatched",
        "Package arrived at regional sorting facility",
        "Delivery driver assigned for final mile",
        "Customer confirmed receipt of package",
    ],
    "inventory_change": [
        "Stock level updated after warehouse scan",
        "Replenishment order triggered for low stock item",
        "Inventory adjustment due to return processing",
    ],
    "customer_action": [
        "Customer submitted a support ticket",
        "Customer updated delivery preferences",
        "Customer requested order cancellation",
    ],
    "system_alert": [
        "Service latency exceeded threshold",
        "Database failover initiated",
        "Cache invalidation triggered for stale entries",
    ],
}


def generate_event(event_type=None, entity_id=None, status=None):
    """Generate a single realistic event."""
    etype = event_type or random.choice(EVENT_TYPES)
    eid = entity_id or f"{etype.upper().replace('_', '-')}-{uuid.uuid4().hex[:8].upper()}"

    event = {
        "event_id": f"evt-{uuid.uuid4().hex[:12]}",
        "entity_id": eid,
        "event_type": etype,
        "timestamp": datetime.now(UTC).isoformat(),
        "data": {
            "status": status
            or (random.choice(ORDER_STATUSES) if etype == "order_update" else "active"),
            "description": random.choice(DESCRIPTIONS.get(etype, ["Event occurred"])),
            "metadata": {},
        },
    }

    # Add type-specific metadata
    if etype == "order_update":
        event["data"]["metadata"] = {
            "carrier": random.choice(CARRIERS),
            "tracking": f"TRK{''.join(random.choices(string.digits, k=10))}",
            "weight_kg": str(round(random.uniform(0.5, 25.0), 1)),
        }
    elif etype == "inventory_change":
        event["data"]["metadata"] = {
            "sku": f"SKU-{''.join(random.choices(string.ascii_uppercase + string.digits, k=6))}",
            "quantity_delta": str(random.randint(-50, 200)),
            "warehouse": random.choice(["EU-WEST", "US-EAST", "ME-QATAR", "APAC-SG"]),
        }

    return event


def main():
    parser = argparse.ArgumentParser(description="Push test events to Kinesis")
    parser.add_argument("--count", type=int, default=10, help="Number of events to generate")
    parser.add_argument(
        "--stream", type=str, default="realtime-rag-stream", help="Kinesis stream name"
    )
    parser.add_argument(
        "--type", type=str, default=None, help="Event type (order_update, inventory_change, etc.)"
    )
    parser.add_argument("--entity-id", type=str, default=None, help="Specific entity ID")
    parser.add_argument("--status", type=str, default=None, help="Specific status value")
    parser.add_argument("--region", type=str, default=None, help="AWS region")
    args = parser.parse_args()

    kinesis = boto3.client("kinesis", region_name=args.region)

    print(f"Generating {args.count} events → Kinesis stream '{args.stream}'")

    records = []
    for _ in range(args.count):
        event = generate_event(
            event_type=args.type,
            entity_id=args.entity_id,
            status=args.status,
        )
        records.append(
            {
                "Data": json.dumps(event).encode("utf-8"),
                "PartitionKey": event["entity_id"],
            }
        )

        # Kinesis PutRecords supports max 500 records per call
        if len(records) >= 500:
            response = kinesis.put_records(StreamName=args.stream, Records=records)
            failed = response.get("FailedRecordCount", 0)
            print(f"  Sent batch of {len(records)} ({failed} failed)")
            records = []

    # Send remaining records
    if records:
        response = kinesis.put_records(StreamName=args.stream, Records=records)
        failed = response.get("FailedRecordCount", 0)
        print(f"  Sent batch of {len(records)} ({failed} failed)")

    print(f"Done. {args.count} events pushed to '{args.stream}'")


if __name__ == "__main__":
    main()
