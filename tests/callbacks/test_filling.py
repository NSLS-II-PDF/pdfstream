"""Tests for the TiledFiller callback with stream_resource/stream_datum documents."""
import numpy as np
from tiled.client import from_uri

from pdfstream.callbacks.composer import gen_stream, gen_stream_external
from pdfstream.callbacks.filling import TiledFiller

# Keys that compose_run sets internally and should not be passed as metadata
_INTERNAL_START_KEYS = {"uid", "time", "versions"}


def _start_metadata(run):
    """Extract user metadata from a run's start doc, excluding internal keys."""
    return {k: v for k, v in run.start.items() if k not in _INTERNAL_START_KEYS}


def test_tiled_filler_stream_resource(db_with_dark_and_light, tiled_server):
    """Test that TiledFiller fills events when data uses stream_resource/stream_datum.

    Simulates the production flow where:
    1. Raw data is already ingested into tiled (via the fixture)
    2. ZMQ delivers unfilled documents with stream_resource/stream_datum references
    3. TiledFiller reads the actual data from tiled and fills the events
    4. Downstream subscribers receive filled events
    """
    db = db_with_dark_and_light
    # Get the light run (last one inserted)
    light_run = list(db.values())[-1]
    run_uid = light_run.start["uid"]

    # Read the expected image data from tiled
    expected_image = np.asarray(light_run["primary"]["pe1_image"][0])

    # Generate an unfilled document stream with stream_resource/stream_datum,
    # using the same run UID so TiledFiller can look it up in tiled
    data_lst = [{"pe1_image": expected_image}]
    metadata = _start_metadata(light_run)

    doc_stream = list(gen_stream_external(
        data_lst, metadata, external_keys={"pe1_image"}, uid=run_uid
    ))

    # Verify the doc stream contains the expected document types
    doc_names = [name for name, _ in doc_stream]
    assert "stream_resource" in doc_names
    assert "stream_datum" in doc_names

    # Verify events have unfilled pe1_image
    events_in = [(name, doc) for name, doc in doc_stream if name == "event"]
    assert len(events_in) == 1
    assert events_in[0][1]["filled"]["pe1_image"] is False

    # Set up TiledFiller and capture filled output
    filler_client = from_uri(tiled_server.uri)
    filler = TiledFiller(filler_client)
    filled_events = []

    def capture(name, doc):
        if name == "event":
            filled_events.append(doc)

    filler.subscribe(capture)

    # Feed the unfilled doc stream through the filler
    for name, doc in doc_stream:
        filler(name, doc)

    # Verify the filler produced a filled event
    assert len(filled_events) == 1
    event = filled_events[0]
    assert event["filled"]["pe1_image"] is True
    assert isinstance(event["data"]["pe1_image"], np.ndarray)
    assert np.allclose(event["data"]["pe1_image"], expected_image)


def test_tiled_filler_multiple_events(db_with_dark_and_scan, tiled_server):
    """Test TiledFiller fills multiple events from a scan with stream_resource/stream_datum."""
    db = db_with_dark_and_scan
    # Find the scan run (not the dark frame)
    scan_run = None
    for run in db.values():
        if not run.start.get("dark_frame") and "pe1_image" in run.get("primary", {}).keys():
            if "temperature" in run["primary"].keys():
                scan_run = run
                break
    assert scan_run is not None, "Could not find scan run in db_with_dark_and_scan"
    run_uid = scan_run.start["uid"]
    stream = scan_run["primary"]

    n_events = len(stream["pe1_image"].read())
    expected_images = [np.asarray(stream["pe1_image"][i]) for i in range(n_events)]
    expected_temps = stream["temperature"].read()

    # Build data list matching what's in tiled
    data_lst = [
        {"pe1_image": expected_images[i], "temperature": float(expected_temps[i])}
        for i in range(n_events)
    ]

    metadata = _start_metadata(scan_run)

    doc_stream = list(gen_stream_external(
        data_lst, metadata, external_keys={"pe1_image"}, uid=run_uid
    ))

    # Verify stream_datum count matches events
    sd_count = sum(1 for name, _ in doc_stream if name == "stream_datum")
    assert sd_count == n_events

    # Set up TiledFiller
    filler_client = from_uri(tiled_server.uri)
    filler = TiledFiller(filler_client)
    filled_events = []

    def capture(name, doc):
        if name == "event":
            filled_events.append(doc)

    filler.subscribe(capture)

    for name, doc in doc_stream:
        filler(name, doc)

    assert len(filled_events) == n_events
    for i, event in enumerate(filled_events):
        assert event["filled"]["pe1_image"] is True
        assert np.allclose(event["data"]["pe1_image"], expected_images[i])
        # temperature is internal (not external), should pass through as-is
        assert event["data"]["temperature"] == float(expected_temps[i])


def test_tiled_filler_forwards_all_docs(db_with_dark_and_light, tiled_server):
    """Test that TiledFiller forwards start, descriptor, event, and stop to subscribers."""
    db = db_with_dark_and_light
    light_run = list(db.values())[-1]
    run_uid = light_run.start["uid"]
    expected_image = np.asarray(light_run["primary"]["pe1_image"][0])

    data_lst = [{"pe1_image": expected_image}]
    metadata = _start_metadata(light_run)

    doc_stream = list(gen_stream_external(
        data_lst, metadata, external_keys={"pe1_image"}, uid=run_uid
    ))

    filler_client = from_uri(tiled_server.uri)
    filler = TiledFiller(filler_client)
    received = []

    def capture(name, doc):
        received.append(name)

    filler.subscribe(capture)

    for name, doc in doc_stream:
        filler(name, doc)

    # TiledFiller should forward start, descriptor, event, stop
    # (stream_resource and stream_datum are not forwarded by CallbackBase)
    assert "start" in received
    assert "descriptor" in received
    assert "event" in received
    assert "stop" in received
