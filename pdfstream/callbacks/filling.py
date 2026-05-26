"""Subscribe to a tiled dataset stream to receive data as it is written."""
import time
import uuid

import numpy as np
from bluesky.callbacks import CallbackBase

from pdfstream.io import server_message


class TiledSubscriber(CallbackBase):
    """A callback that subscribes to a tiled dataset using tiled's streaming API.

    Instead of filling event documents after the fact, this subscriber uses tiled's
    built-in streaming support (``node.subscribe()`` + ``new_data`` callbacks) to receive
    data as it is written to the specified data_key under the primary stream.

    On each ``new_data`` update, it emits a synthetic event document downstream containing
    the array data.

    Parameters
    ----------
    tiled_client :
        A tiled client connected to the raw data catalog.
    data_key :
        The name of the dataset under the stream to subscribe to (e.g. "pe1_image").
    stream_name :
        The stream name to subscribe to. Default is "primary".
    max_retries :
        Maximum number of retries when looking up the run in tiled.
    retry_delay :
        Seconds to wait between retries.
    """

    def __init__(self, tiled_client, data_key, stream_name="primary",
                 max_retries=20, retry_delay=1.0):
        super().__init__()
        self.tiled_client = tiled_client
        self.data_key = data_key
        self.stream_name = stream_name
        self._run = None
        self._uid = None
        self._desc_uid = None
        self._subscribers = []
        self._max_retries = max_retries
        self._retry_delay = retry_delay
        self._subscription = None
        self._seq_num = 0

    def subscribe(self, callback):
        """Subscribe a callback to receive documents."""
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
                server_message(f"TiledSubscriber: found run '{self._uid}' on attempt {attempt + 1}")
                return True
            except KeyError:
                if attempt < self._max_retries - 1:
                    time.sleep(self._retry_delay)
        server_message(f"TiledSubscriber: FAILED to find run '{self._uid}' after {self._max_retries} attempts")
        self._run = None
        return False

    def _on_new_data(self, update):
        """Handle a new_data update from the tiled streaming subscription."""
        self._seq_num += 1
        arr = np.asarray(update.data())
        server_message(f"TiledSubscriber: received '{self.data_key}' seq_num={self._seq_num} "
                       f"shape={arr.shape}")
        event_doc = {
            "uid": str(uuid.uuid4()),
            "descriptor": self._desc_uid,
            "seq_num": self._seq_num,
            "time": time.time(),
            "data": {self.data_key: arr},
            "timestamps": {self.data_key: time.time()},
            "filled": {self.data_key: True},
        }
        self._emit("event", event_doc)

    def _start_subscription(self):
        """Subscribe to the dataset in tiled using the streaming API, with retries."""
        for attempt in range(self._max_retries):
            try:
                # Refresh the run to pick up newly ingested streams
                self._run = self.tiled_client[self._uid]
                stream = self._run[self.stream_name]
                if stream is None:
                    raise KeyError(f"Stream '{self.stream_name}' not yet available")
                dataset = stream[self.data_key]
                if dataset is None:
                    raise KeyError(f"Dataset '{self.data_key}' not yet available")
                self._subscription = dataset.subscribe()
                self._subscription.new_data.add_callback(self._on_new_data)
                # Start from 0 to catch up on any data written before we subscribed
                self._subscription.start_in_thread(0)
                server_message(f"TiledSubscriber: streaming '{self.data_key}' from "
                               f"run '{self._uid}' stream '{self.stream_name}' "
                               f"(attempt {attempt + 1})")
                return
            except (KeyError, TypeError) as e:
                if attempt < self._max_retries - 1:
                    server_message(f"TiledSubscriber: stream/dataset not ready, "
                                   f"retrying ({attempt + 1}/{self._max_retries}): {e}")
                    time.sleep(self._retry_delay)
                else:
                    server_message(f"TiledSubscriber: FAILED to start subscription "
                                   f"after {self._max_retries} attempts: {e}")

    def start(self, doc):
        self._uid = doc["uid"]
        self._run = None
        self._seq_num = 0
        self._subscription = None
        self._emit("start", doc)

        # Look up the run in tiled
        if not self._lookup_run():
            server_message("TiledSubscriber: cannot proceed without run in tiled")
            return

    def descriptor(self, doc):
        if doc.get("name", "primary") == self.stream_name:
            self._desc_uid = doc["uid"]
        self._emit("descriptor", doc)

        # Start the tiled streaming subscription after we have the descriptor
        if self._desc_uid and self._subscription is None and self._run is not None:
            self._start_subscription()

    def event(self, doc):
        # Events from ZMQ are ignored — data comes from the tiled stream instead
        pass

    def stop(self, doc):
        if self._subscription is not None:
            self._subscription.disconnect()
            server_message("TiledSubscriber: subscription disconnected")
            self._subscription = None
        self._emit("stop", doc)
        self._run = None
        self._uid = None
        self._desc_uid = None
        server_message("TiledSubscriber: run complete")
