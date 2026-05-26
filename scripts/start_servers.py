#!/usr/bin/env python
"""Start up xpd_server, xpdsave_server, and xpdvis_server on localhost.

Each server runs in its own process. The xpd_server listens for raw data on
the raw proxy port, processes it, and publishes analyzed results through a
second ZMQ proxy. The save and vis servers subscribe to that analyzed proxy.

Port assignments (all on localhost):
    5568 - raw data proxy OUT -> xpd_server listens here
    5567 - analyzed proxy IN  -> xpd_server Publisher connects here
    5566 - analyzed proxy OUT -> xpdsave/xpdvis servers listen here

Usage:
    # Use default configs (no arguments required):
    python scripts/start_servers.py

    # Provide your own configs:
    python scripts/start_servers.py --xpd-config my_xpd.ini --save-config my_save.ini --vis-config my_vis.ini

    # Skip individual servers:
    python scripts/start_servers.py --no-save --no-vis
"""
import argparse
import logging
import multiprocessing
import os
import signal
import sys
import tempfile
import time
import warnings
from pathlib import Path

logger = logging.getLogger("pdfstream.servers")

# Default port assignments
RAW_PROXY_PORT = 5568       # proxy publishes raw data here; xpd_server subscribes
ANALYZED_IN_PORT = 5567     # analyzed proxy IN: xpd_server Publisher connects here
ANALYZED_OUT_PORT = 5566    # analyzed proxy OUT: save/vis servers subscribe here
HOST = "localhost"
ANALYZED_PREFIX = "an"

# Default data directories
DEFAULT_DATA_DIR = Path("~/pdfstream_data").expanduser()
DEFAULT_CALIB_DIR = Path("~/pdfstream_calibration").expanduser()


def _default_xpd_config():
    """Return the contents of a reasonable default xpd_server config."""
    return f"""\
[BASIC]
name = xpd
version = 1.0.0

[FUNCTIONALITY]
do_calibration = True
dump_to_db = False
export_files = False
visualize_data = False
send_messages = True

[LISTEN TO]
host = {HOST}
port = {RAW_PROXY_PORT}
prefix = raw

[PUBLISH TO]
host = {HOST}
port = {ANALYZED_IN_PORT}
prefix = {ANALYZED_PREFIX}

[DATABASE]
raw_db = http://localhost:8008
raw_db_api_key = test
# an_db = http://localhost:8001

[METADATA]
dk_identifier = dark_frame
calib_identifier = is_calibration
dk_id_key = sc_dk_field_uid
calibration_md_key = calibration_md
composition_key = sample_composition
wavelength_key = bt_wavelength
bkgd_sample_name_key = bkgd_sample_name
sample_name_key = sample_name
detector_key = detector
calibrant_key = sample_composition
data_key = xsp

[CALIBRATION]
calib_base = {DEFAULT_CALIB_DIR}
default_calibrant = Ni

[ANALYSIS]
alpha = 2.0
edge = 20
lower_thresh = 0.0
npt = 1024
correctSolidAngle = False
polarization_factor = 0.99
rpoly = 1.0
qmaxinst = 24.0
qmin = 0.0
qmax = 22.0
rmin = 0.0
rmax = 30.0
rstep = 0.01

[SUITCASE]
tiff_base = {DEFAULT_DATA_DIR}
exports = tiff,yaml,csv,txt
file_prefix = {{start[original_run_uid]}}_{{start[readable_time]}}_
"""


def _default_save_config():
    """Return the contents of a reasonable default xpdsave_server config."""
    return f"""\
[BASIC]
name = xpdsave
version = 1.0.0

[LISTEN TO]
host = {HOST}
port = {ANALYZED_OUT_PORT}
prefix = {ANALYZED_PREFIX}

[SUITCASE]
tiff_base = {DEFAULT_DATA_DIR}
exports = tiff,yaml,csv,txt
file_prefix = {{start[original_run_uid]}}_{{start[readable_time]}}_
"""


def _default_vis_config():
    """Return the contents of a reasonable default xpdvis_server config."""
    return f"""\
[BASIC]
name = xpdvis
version = 1.0.0

[LISTEN TO]
host = {HOST}
port = {ANALYZED_OUT_PORT}
prefix = {ANALYZED_PREFIX}

[VISUALIZATION]
visualizers = dk_sub_image,masked_image,chi,iq,sq,fq,gr,chi_max,chi_argmax,gr_max,gr_argmax
"""


def _write_default_config(tmpdir, name, content):
    """Write a default config to a temp file and return the path."""
    path = os.path.join(tmpdir, f"{name}.ini")
    with open(path, "w") as f:
        f.write(content)
    return path


def run_analyzed_proxy():
    """Run a ZMQ proxy for analyzed data (Publisher → Proxy → RemoteDispatchers)."""
    logger.debug("Importing bluesky.callbacks.zmq.Proxy")
    from bluesky.callbacks.zmq import Proxy
    logger.info(f"Analyzed proxy binding in={HOST}:{ANALYZED_IN_PORT} out={HOST}:{ANALYZED_OUT_PORT}")
    proxy = Proxy(
        in_address=(HOST, ANALYZED_IN_PORT),
        out_address=(HOST, ANALYZED_OUT_PORT),
    )
    logger.debug("Analyzed proxy entering event loop")
    proxy.start()


def run_xpd_server(cfg_file):
    """Run the XPD analysis server."""
    warnings.simplefilter("ignore")
    logger.debug(f"Loading xpd_server config from: {cfg_file}")
    from pdfstream.servers.xpd_server import XPDServerConfig, XPDServer
    config = XPDServerConfig()
    config.read(cfg_file)
    logger.info(f"xpd_server config loaded: calibration={config.functionality.get('do_calibration')}, "
                f"export={config.functionality.get('export_files')}, "
                f"visualize={config.functionality.get('visualize_data')}")
    server = XPDServer(config)
    if config.functionality["visualize_data"]:
        logger.debug("Installing Qt kicker for xpd_server visualization")
        server.install_qt_kicker()
    logger.info("xpd_server starting event loop")
    server.start()


def run_save_server(cfg_file):
    """Run the XPD save server."""
    warnings.simplefilter("ignore")
    logger.debug(f"Loading xpdsave_server config from: {cfg_file}")
    from pdfstream.servers.xpdsave_server import XPDSaveServerConfig, XPDSaveServer
    config = XPDSaveServerConfig()
    config.read(cfg_file)
    logger.info("xpdsave_server config loaded successfully")
    server = XPDSaveServer(config)
    logger.info("xpdsave_server starting event loop")
    server.start()


def run_vis_server(cfg_file):
    """Run the XPD visualization server."""
    warnings.simplefilter("ignore")
    logger.debug(f"Loading xpdvis_server config from: {cfg_file}")
    from pdfstream.servers.xpdvis_server import XPDVisServerConfig, XPDVisServer
    config = XPDVisServerConfig()
    config.read(cfg_file)
    logger.info("xpdvis_server config loaded successfully")
    server = XPDVisServer(config)
    logger.debug("Installing Qt kicker for xpdvis_server")
    server.install_qt_kicker()
    logger.info("xpdvis_server starting event loop")
    server.start()


def main():
    parser = argparse.ArgumentParser(
        description="Start pdfstream analysis servers on localhost with sensible defaults."
    )
    parser.add_argument("--xpd-config", default=None,
                        help="Path to the xpd_server .ini config file (generated if omitted).")
    parser.add_argument("--save-config", default=None,
                        help="Path to the xpdsave_server .ini config (generated if omitted).")
    parser.add_argument("--vis-config", default=None,
                        help="Path to the xpdvis_server .ini config (generated if omitted).")
    parser.add_argument("--no-save", action="store_true", help="Skip starting the save server.")
    parser.add_argument("--no-vis", action="store_true", help="Skip starting the vis server.")
    parser.add_argument("--print-configs", action="store_true",
                        help="Print the default configs to stdout and exit (useful as a starting point).")
    parser.add_argument("-v", "--verbose", action="count", default=0,
                        help="Increase verbosity (-v for INFO, -vv for DEBUG).")
    args = parser.parse_args()

    # Configure logging based on verbosity
    if args.verbose >= 2:
        log_level = logging.DEBUG
    elif args.verbose >= 1:
        log_level = logging.INFO
    else:
        log_level = logging.WARNING
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.debug(f"Verbosity level: {args.verbose} (log_level={logging.getLevelName(log_level)})")

    if args.print_configs:
        print("=" * 60)
        print("  xpd_server.ini")
        print("=" * 60)
        print(_default_xpd_config())
        print("=" * 60)
        print("  xpdsave_server.ini")
        print("=" * 60)
        print(_default_save_config())
        print("=" * 60)
        print("  xpdvis_server.ini")
        print("=" * 60)
        print(_default_vis_config())
        return

    # Generate default configs for any that weren't provided
    tmpdir = tempfile.mkdtemp(prefix="pdfstream_configs_")
    logger.debug(f"Temporary config directory: {tmpdir}")

    xpd_cfg = args.xpd_config or _write_default_config(tmpdir, "xpd_server", _default_xpd_config())
    save_cfg = args.save_config or _write_default_config(tmpdir, "xpdsave_server", _default_save_config())
    vis_cfg = args.vis_config or _write_default_config(tmpdir, "xpdvis_server", _default_vis_config())
    logger.debug(f"Config paths: xpd={xpd_cfg}, save={save_cfg}, vis={vis_cfg}")

    if not args.xpd_config:
        print(f"Using generated xpd config:  {xpd_cfg}")
    if not args.save_config and not args.no_save:
        print(f"Using generated save config: {save_cfg}")
    if not args.vis_config and not args.no_vis:
        print(f"Using generated vis config:  {vis_cfg}")

    processes = []

    # Start the analyzed data proxy first (Publisher connects to in_port, subscribers to out_port)
    if not args.no_save or not args.no_vis:
        print(f"\nStarting analyzed proxy   in={HOST}:{ANALYZED_IN_PORT}  out={HOST}:{ANALYZED_OUT_PORT}")
        p_proxy = multiprocessing.Process(target=run_analyzed_proxy, name="analyzed_proxy", daemon=True)
        p_proxy.start()
        processes.append(p_proxy)
        logger.info(f"Analyzed proxy process started (pid={p_proxy.pid})")

    print(f"Starting xpd_server      listening={HOST}:{RAW_PROXY_PORT}  publishing={HOST}:{ANALYZED_IN_PORT}")
    logger.debug(f"xpd_server config: {xpd_cfg}")
    p_xpd = multiprocessing.Process(target=run_xpd_server, args=(xpd_cfg,), name="xpd_server")
    p_xpd.start()
    processes.append(p_xpd)
    logger.info(f"xpd_server process started (pid={p_xpd.pid})")

    if not args.no_save:
        print(f"Starting xpdsave_server  listening={HOST}:{ANALYZED_OUT_PORT}")
        logger.debug(f"xpdsave_server config: {save_cfg}")
        p_save = multiprocessing.Process(target=run_save_server, args=(save_cfg,), name="xpdsave_server")
        p_save.start()
        processes.append(p_save)
        logger.info(f"xpdsave_server process started (pid={p_save.pid})")
    else:
        logger.info("Skipping xpdsave_server (--no-save)")

    if not args.no_vis:
        print(f"Starting xpdvis_server   listening={HOST}:{ANALYZED_OUT_PORT}")
        logger.debug(f"xpdvis_server config: {vis_cfg}")
        p_vis = multiprocessing.Process(target=run_vis_server, args=(vis_cfg,), name="xpdvis_server")
        p_vis.start()
        processes.append(p_vis)
        logger.info(f"xpdvis_server process started (pid={p_vis.pid})")
    else:
        logger.info("Skipping xpdvis_server (--no-vis)")

    print(f"\n{len(processes)} server(s) running. Press Ctrl+C to stop all.")
    logger.debug(f"All process PIDs: {[p.pid for p in processes]}")

    def shutdown(sig, frame):
        print("\nShutting down servers...")
        for p in processes:
            logger.info(f"Terminating {p.name} (pid={p.pid})")
            p.terminate()
        for p in processes:
            p.join(timeout=5)
            if p.is_alive():
                logger.warning(f"{p.name} (pid={p.pid}) did not exit in time, killing")
                p.kill()
            else:
                logger.info(f"{p.name} (pid={p.pid}) exited with code {p.exitcode}")
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    for p in processes:
        p.join()


if __name__ == "__main__":
    main()
