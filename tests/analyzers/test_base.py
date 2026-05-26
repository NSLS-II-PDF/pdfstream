import pytest
from bluesky_tiled_plugins import TiledWriter
from bluesky_tiled_plugins.exporters import json_seq_exporter
from tiled.client import from_uri
from tiled.media_type_registration import default_serialization_registry
from tiled.server import SimpleTiledServer

# Register the json-seq exporter so run.documents() works with SimpleTiledServer
default_serialization_registry.register("BlueskyRun", "application/json-seq", json_seq_exporter)

import pdfstream.analyzers.base as mod
from pdfstream.callbacks.composer import gen_stream


@pytest.fixture(scope="function")
def db_with_fake_an(tmp_path):
    """A tiled catalog that has a fake analysis run."""
    server = SimpleTiledServer(readable_storage=[str(tmp_path)])
    client = from_uri(server.uri)
    tw = TiledWriter(client, batch_size=1)
    for name, doc in gen_stream([], {"an_config": {"SECTION": {"key": "value"}}}):
        tw(name, doc)
    yield client
    server.close()


def test_AnalyzerConfig(db_with_fake_an):
    db = db_with_fake_an
    config = mod.AnalyzerConfig()
    config.read_run(db.values().last())
    assert config.sections() == ["SECTION"]
    assert config["SECTION"]["key"] == "value"


def test_Analyzer(db_with_fake_an):
    db = db_with_fake_an
    analyzer = mod.Analyzer()
    analyzer.analyze(db.values().last())
