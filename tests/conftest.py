"""Configuration of pytest."""
import json
import uuid
from pathlib import Path

import matplotlib.pyplot as plt
import numpy
import numpy as np
import pyFAI
import pytest
from bluesky_tiled_plugins import TiledWriter
from bluesky_tiled_plugins.exporters import json_seq_exporter
from diffpy.pdfgetx import PDFConfig, PDFGetter
from pkg_resources import resource_filename
from tiled.client import from_uri
from tiled.media_type_registration import default_serialization_registry
from tiled.server import SimpleTiledServer

# Register the json-seq exporter so run.documents() works with SimpleTiledServer
default_serialization_registry.register("BlueskyRun", "application/json-seq", json_seq_exporter)

from pdfstream.callbacks.composer import gen_stream
from pdfstream.io import load_img, load_array

# do not show any figures in test otherwise they will block the tests
plt.ioff()

NI_PONI_FILE = resource_filename('tests', 'test_data/Ni_poni_file.poni')
NI_GR_FILE = resource_filename('tests', 'test_data/Ni_gr_file.gr')
NI_CHI_FILE = resource_filename('tests', 'test_data/Ni_chi_file.chi')
NI_FGR_FILE = resource_filename('tests', 'test_data/Ni_fgr_file.fgr')
NI_IMG_FILE = resource_filename('tests', 'test_data/Ni_img_file.tiff')
MASK_FILE = resource_filename("tests", "test_data/mask_file.npy")
KAPTON_IMG_FILE = resource_filename('tests', 'test_data/Kapton_img_file.tiff')
BLACK_IMG_FILE = resource_filename('tests', 'test_data/black_img.tiff')
WHITE_IMG_FILE = resource_filename('tests', 'test_data/white_img.tiff')
NI_IMG = load_img(NI_IMG_FILE)
NI_FRAMES = numpy.expand_dims(NI_IMG, 0)
KAPTON_IMG = load_img(KAPTON_IMG_FILE)
NI_GR = load_array(NI_GR_FILE)
NI_CHI = load_array(NI_CHI_FILE)
NI_FGR = load_array(NI_FGR_FILE)
NI_CONFIG = PDFConfig()
NI_CONFIG.readConfig(NI_GR_FILE)
NI_PDFGETTER = PDFGetter(NI_CONFIG)
AI = pyFAI.load(NI_PONI_FILE)
MASK = numpy.load(MASK_FILE)
BLACK_IMG = load_img(BLACK_IMG_FILE)
WHITE_IMG = load_img(WHITE_IMG_FILE)
START_DOC_FILE = resource_filename('tests', 'test_data/start.json')
with Path(START_DOC_FILE).open("r") as f:
    START_DOC = json.load(f)

DB = {
    'Ni_img_file': NI_IMG_FILE,
    'Ni_img': NI_IMG,
    'Kapton_img_file': KAPTON_IMG_FILE,
    'Kapton_img': KAPTON_IMG,
    'Ni_poni_file': NI_PONI_FILE,
    'Ni_gr_file': NI_GR_FILE,
    'Ni_chi_file': NI_CHI_FILE,
    'Ni_fgr_file': NI_FGR_FILE,
    'ai': AI,
    'Ni_gr': NI_GR,
    'Ni_chi': NI_CHI,
    'Ni_fgr': NI_FGR,
    'black_img_file': BLACK_IMG_FILE,
    'white_img_file': WHITE_IMG_FILE,
    'black_img': BLACK_IMG,
    'white_img': WHITE_IMG,
    'Ni_config': NI_CONFIG,
    'Ni_pdfgetter': NI_PDFGETTER,
    'mask_file': MASK_FILE,
    'mask': MASK,
    'start_doc': START_DOC
}


@pytest.fixture(scope="session")
def test_data():
    """Test configs."""
    return DB


@pytest.fixture(scope="function")
def simple_stream():
    return gen_stream([{"x": 1}], {})


@pytest.fixture(
    scope="function",
    params=[
        {"hints": {}},
        {"hints": {"dimensions": [(["x0"], "primary")]}},
        {"hints": {"dimensions": [(["x0"], "primary"), (["x1"], "primary")]}},
    ]
)
def ymax_stream(request):
    data = [
        {"ymax": 0, "x0": 0, "x1": 0},
        {"ymax": 1, "x0": 1, "x1": 1},
        {"ymax": 2, "x0": 2, "x1": 2}
    ]
    return gen_stream(data, request.param)


@pytest.fixture(scope="function")
def array_stream():
    return gen_stream(
        [
            {"x0": np.zeros(5), "x1": np.zeros(5)},
            {"x0": np.ones(5), "x1": np.ones(5)},
        ],
        {}
    )


@pytest.fixture(scope="session")
def tiled_server(tmp_path_factory):
    """A shared tiled server for all test fixtures."""
    tmp_dir = tmp_path_factory.mktemp("tiled_data")
    server = SimpleTiledServer(readable_storage=[str(tmp_dir)])
    yield server
    server.close()


def _insert_docs(tiled_client, doc_stream):
    """Insert documents from a gen_stream into a tiled server via TiledWriter."""
    tw = TiledWriter(tiled_client, batch_size=1)
    for name, doc in doc_stream:
        tw(name, doc)


@pytest.fixture(scope="session")
def db_with_dark_and_light(tiled_server):
    """A tiled catalog with a dark run and a light run inside."""
    client = from_uri(tiled_server.uri)
    dark_data = [{"pe1_image": np.zeros_like(NI_FRAMES)}]
    dark_uid = str(uuid.uuid4())
    _insert_docs(client, gen_stream(dark_data, {"dark_frame": True}, uid=dark_uid))
    light_data = [{"pe1_image": NI_FRAMES}]
    _insert_docs(client, gen_stream(light_data, dict(**START_DOC, sc_dk_field_uid=dark_uid)))
    return client


@pytest.fixture(scope="session")
def db_with_img_and_bg_img(tiled_server):
    """A tiled catalog with a dark image, a background image run and a data image run inside."""
    client = from_uri(tiled_server.uri)
    sample_name = "Kapton"
    dk_uid = str(uuid.uuid4())
    dk_meta = {"dark_frame": True}
    dk_data = [{"pe1_image": np.ones_like(NI_FRAMES)}]
    bg_meta = {"sample_name": sample_name, "sc_dk_field_uid": dk_uid}
    bg_data = [{"pe1_image": 2 * np.ones_like(NI_FRAMES)}]
    img_data = [{"pe1_image": 2 * np.ones_like(NI_FRAMES) + NI_FRAMES}]
    img_meta = dict(**START_DOC, bkgd_sample_name=sample_name, sc_dk_field_uid=dk_uid, sample_name="Ni")
    _insert_docs(client, gen_stream(dk_data, dk_meta, uid=dk_uid))
    _insert_docs(client, gen_stream(bg_data, bg_meta))
    _insert_docs(client, gen_stream(img_data, img_meta))
    return client


@pytest.fixture(scope="session")
def db_with_dark_and_scan(tiled_server):
    """A tiled catalog with a dark run and a motor scan inside."""
    client = from_uri(tiled_server.uri)
    dark_data = [{"pe1_image": np.zeros_like(NI_FRAMES)}]
    dark_uid = str(uuid.uuid4())
    _insert_docs(client, gen_stream(dark_data, {"dark_frame": True}, uid=dark_uid))
    light_data = [
        {"pe1_image": NI_FRAMES, "temperature": 0},
        {"pe1_image": NI_FRAMES, "temperature": 1},
        {"pe1_image": NI_FRAMES, "temperature": 3}
    ]
    start = dict(**START_DOC)
    start.update(
        {
            "sc_dk_field_uid": dark_uid,
            "hints": {"dimensions": [(["temperature"], "primary")]},
            "sample_name": "Ni"
        }
    )
    _insert_docs(client, gen_stream(light_data, start))
    return client


@pytest.fixture(scope="session")
def db_with_dark_and_calib(tiled_server):
    """A tiled catalog with a dark run and a calibration run inside."""
    client = from_uri(tiled_server.uri)
    dark_data = [{"pe1_image": np.zeros_like(NI_FRAMES)}]
    dark_uid = str(uuid.uuid4())
    _insert_docs(client, gen_stream(dark_data, {"dark_frame": True}, uid=dark_uid))
    light_data = [{"pe1_image": NI_FRAMES}]
    _insert_docs(client, gen_stream(
        light_data, dict(
            sample_composition="Ni",
            sc_dk_field_uid=dark_uid,
            detector="perkin_elmer",
            is_calibration=True,
            bt_wavelength=0.1917
        )
    ))
    return client


@pytest.fixture(scope="session")
def db_with_dark_bg_no_calib(tiled_server):
    """A tiled catalog with a dark image, a background image run and a data image run without calibration."""
    client = from_uri(tiled_server.uri)
    sample_name = "Kapton"
    dk_uid = str(uuid.uuid4())
    dk_meta = {"dark_frame": True}
    dk_data = [{"pe1_image": np.ones_like(NI_FRAMES)}]
    bg_meta = {"sample_name": sample_name, "sc_dk_field_uid": dk_uid}
    bg_data = [{"pe1_image": 2 * np.ones_like(NI_FRAMES)}]
    img_data = [{"pe1_image": 2 * np.ones_like(NI_FRAMES) + NI_FRAMES}]
    img_meta = dict(**START_DOC, bkgd_sample_name=sample_name, sc_dk_field_uid=dk_uid, sample_name="Ni")
    img_meta.pop("calibration_md")
    _insert_docs(client, gen_stream(dk_data, dk_meta, uid=dk_uid))
    _insert_docs(client, gen_stream(bg_data, bg_meta))
    _insert_docs(client, gen_stream(img_data, img_meta))
    return client
