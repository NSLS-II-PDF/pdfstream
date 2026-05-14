"""Fill event documents using a tiled client to resolve external data references."""
import time

import numpy as np
from bluesky.callbacks import CallbackBase

from pdfstream.io import server_message


class TiledFiller(CallbackBase):
    """A callback that fills unfilled event documents by reading data from a tiled server.

    When events arrive with datum_id references (unfilled external data), this callback
    reads the actual array data from the tiled server and replaces the references with
    filled numpy arrays before passing them to downstream subscribers.

    Parameters
    ----------
    tiled_client :
        A tiled client connected to the raw data catalog. The raw data must already be
        ingested into this tiled server before events arrive via ZMQ.
    max_retries :
        Maximum number of retries when looking up the run in tiled.
    retry_delay :
        Seconds to wait between retries.
    """

    def __init__(self, tiled_client, max_retries=20, retry_delay=1.0):
        super().__init__()
        self.tiled_client = tiled_client
        self._run = None
        self._uid = None
        self._external_keys = {}  # descriptor_uid -> set of external data_key names
        self._stream_names = {}  # descriptor_uid -> stream name
        self._subscribers = []
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    def subscribe(self, callback):
        """Subscribe a callback to receive filled documents."""
        self._subscribers.append(callback)

    def _emit(self, name, doc):
        """Forward a document to all subscribers."""
        for cb in self._subscribers:
            cb(name, doc)

    def _lookup_run(self):
        """Try to look up the current run in tiled, with retries."""
        for attempt in range(self._max_retries):
            try:
                self._run = self.tiled_client[self._uid]
                server_message(f"TiledFiller: found run '{self._uid}' on attempt {attempt + 1}")
                return
            except KeyError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
        server_message(f"TiledFiller: FAILED to find run '{self._uid}' after {self._max_retries} attempts")
        self._run = None

    def start(self, doc):
        self._uid = doc["uid"]
        self._run = None
        self._emit("start", doc)

    def descriptor(self, doc):
        desc_uid = doc["uid"]
        self._stream_names[desc_uid] = doc.get("name", "primary")
        external_keys = set()
        for key, info in doc.get("data_keys", {}).items():
            if info.get("external"):
                external_keys.add(key)
        self._external_keys[desc_uid] = external_keys
        self._emit("descriptor", doc)

    def _read_stream_data(self, stream_name, key, idx):
        """Read data from a tiled stream with retries for ingestion lag."""
        for attempt in range(self._max_retries):
            try:
                stream = self._run[stream_name]
                arr = np.asarray(stream[key][idx])
                server_message(f"TiledFiller: read '{key}' idx={idx} on attempt {attempt + 1}")
                return arr
            except (KeyError, IndexError):
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
                    # Refresh the run to pick up newly ingested data
                    try:
                        self._run = self.tiled_client[self._uid]
                    except KeyError:
                        pass
        server_message(f"TiledFiller: FAILED to read '{key}' idx={idx} from stream '{stream_name}'")
        return None

    def event(self, doc):
        desc_uid = doc["descriptor"]
        external_keys = self._external_keys.get(desc_uid, set())
        if external_keys:
            # Lazily look up the run on first event that needs filling
            if self._run is None and self._uid:
                self._lookup_run()
            if self._run is not None:
                doc = dict(doc)
                doc["data"] = dict(doc["data"])
                filled = dict(doc.get("filled", {}))
                stream_name = self._stream_names.get(desc_uid, "primary")
                for key in external_keys:
                    if not filled.get(key, False):
                        arr = self._read_stream_data(stream_name, key, doc["seq_num"] - 1)
                        if arr is not None:
                            doc["data"][key] = arr
                            filled[key] = True
                doc["filled"] = filled
        self._emit("event", doc)

    def stop(self, doc):
        self._emit("stop", doc)
        self._run = None
        self._uid = None
        self._external_keys.clear()
        self._stream_names.clear()
