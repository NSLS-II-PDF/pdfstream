"""Fill event documents using a tiled client to resolve external data references."""
import copy

import numpy as np
from bluesky.callbacks import CallbackBase


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
    """

    def __init__(self, tiled_client):
        super().__init__()
        self.tiled_client = tiled_client
        self._run = None
        self._uid = None
        self._external_keys = {}  # descriptor_uid -> set of external data_key names
        self._stream_names = {}  # descriptor_uid -> stream name

    def start(self, doc):
        self._uid = doc["uid"]
        try:
            self._run = self.tiled_client[self._uid]
        except KeyError:
            self._run = None
        super().start(doc)

    def descriptor(self, doc):
        desc_uid = doc["uid"]
        self._stream_names[desc_uid] = doc.get("name", "primary")
        external_keys = set()
        for key, info in doc.get("data_keys", {}).items():
            if info.get("external"):
                external_keys.add(key)
        self._external_keys[desc_uid] = external_keys
        super().descriptor(doc)

    def event(self, doc):
        desc_uid = doc["descriptor"]
        external_keys = self._external_keys.get(desc_uid, set())
        if external_keys and self._run is not None:
            doc = dict(doc)
            doc["data"] = dict(doc["data"])
            filled = dict(doc.get("filled", {}))
            stream_name = self._stream_names.get(desc_uid, "primary")
            for key in external_keys:
                if not filled.get(key, False):
                    try:
                        stream = self._run[stream_name]
                        # seq_num is 1-indexed, array index is 0-indexed
                        idx = doc["seq_num"] - 1
                        arr = np.asarray(stream[key][idx])
                        doc["data"][key] = arr
                        filled[key] = True
                    except (KeyError, IndexError):
                        pass
            doc["filled"] = filled
        super().event(doc)

    def stop(self, doc):
        self._run = None
        self._uid = None
        self._external_keys.clear()
        self._stream_names.clear()
        super().stop(doc)
