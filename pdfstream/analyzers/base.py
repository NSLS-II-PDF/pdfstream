from configparser import ConfigParser

import numpy as np
from bluesky.callbacks.core import CallbackBase


def iter_documents_filled(run):
    """Iterate filled (name, doc) pairs from a tiled BlueskyRun.

    Reconstructs the document stream by reading column data directly from
    the tiled event streams, since run.documents() does not include data.
    """
    # start
    yield "start", dict(run.start)
    # streams
    for stream_name in run.keys():
        stream = run[stream_name]
        meta = dict(stream.metadata)
        descriptor_doc = {
            "uid": meta.get("uid", ""),
            "run_start": run.start.get("uid", ""),
            "time": meta.get("time", 0),
            "data_keys": meta.get("data_keys", {}),
            "configuration": meta.get("configuration", {}),
            "name": stream_name,
            "hints": meta.get("hints", {}),
            "object_keys": meta.get("object_keys", {}),
        }
        yield "descriptor", descriptor_doc
        # events - read column data
        data_keys = set(meta.get("data_keys", {}).keys())
        columns = list(stream.keys())
        if not columns:
            continue
        n_events = len(stream[columns[0]].read())
        col_data = {col: stream[col].read() for col in columns}
        for i in range(n_events):
            event_data = {}
            event_timestamps = {}
            for key in data_keys:
                if key in col_data:
                    val = col_data[key][i]
                    event_data[key] = np.asarray(val) if hasattr(val, '__array__') else val
                ts_key = f"ts_{key}"
                if ts_key in col_data:
                    event_timestamps[key] = float(col_data[ts_key][i])
            event_doc = {
                "descriptor": descriptor_doc["uid"],
                "uid": f"event-{descriptor_doc['uid']}-{i + 1}",
                "time": float(col_data["time"][i]) if "time" in col_data else 0,
                "seq_num": int(col_data["seq_num"][i]) if "seq_num" in col_data else i + 1,
                "data": event_data,
                "timestamps": event_timestamps,
                "filled": {k: True for k in data_keys},
            }
            yield "event", event_doc
    # stop
    yield "stop", dict(run.stop)


class AnalyzerConfig(ConfigParser):
    """The base class of configuration of analyzers."""

    def read_run(self, run, source="<BlueskyRun>"):
        """Read the configuration from the analysis result in a bluesky run."""
        # see schemas for the key of configuration
        config_dct = run.metadata["start"]["an_config"]
        return self.read_dict(config_dct, source=source)


class Analyzer(CallbackBase):
    """The base class of analyzers."""

    def analyze(self, run):
        """Analyze the data in a bluesky run."""
        for name, doc in iter_documents_filled(run):
            # inject the original_db
            if name == "start":
                doc = dict(doc)
                doc["original_db"] = run.uri
            self.__call__(name, doc)
