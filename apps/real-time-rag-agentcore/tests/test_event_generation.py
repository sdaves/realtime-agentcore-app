"""The sample generator's event contract.

The generator is the pipeline's only producer, so the shape it emits is the
contract every later stage consumes: the streaming job reads these fields by
name, and it embeds the status and description into the text that gets
searched. These assertions pin that shape so a change to it shows up here
rather than as a silent retrieval regression.
"""

from datetime import datetime, timedelta

from generate_events import generate_event


def test_explicit_entity_id_and_status_are_emitted_verbatim():
    event = generate_event(event_type="order_update", entity_id="ORDER-12345", status="shipped")

    assert (event["entity_id"], event["data"]["status"]) == ("ORDER-12345", "shipped")


def test_generated_timestamp_is_an_aware_utc_instant():
    event = generate_event(event_type="order_update")

    assert datetime.fromisoformat(event["timestamp"]).utcoffset() == timedelta(0)


def test_entity_id_defaults_to_a_prefix_derived_from_the_event_type():
    event = generate_event(event_type="inventory_change")

    assert event["entity_id"].startswith("INVENTORY-CHANGE-")


def test_event_id_is_marked_as_an_event():
    event = generate_event(event_type="system_alert")

    assert event["event_id"].startswith("evt-")


def test_order_updates_carry_fulfilment_metadata():
    event = generate_event(event_type="order_update")

    assert set(event["data"]["metadata"]) == {"carrier", "tracking", "weight_kg"}


def test_event_types_without_extra_detail_carry_no_metadata():
    event = generate_event(event_type="system_alert")

    assert event["data"]["metadata"] == {}


def test_event_types_outside_order_updates_report_a_generic_status():
    event = generate_event(event_type="system_alert")

    assert event["data"]["status"] == "active"


def test_each_generated_event_is_distinct():
    first = generate_event(event_type="order_update")
    second = generate_event(event_type="order_update")

    assert first["event_id"] != second["event_id"]
