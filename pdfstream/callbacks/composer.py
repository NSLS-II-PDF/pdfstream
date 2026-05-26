"""Event model run composer from files."""
import time
import typing as tp
import uuid

import numpy as np
from event_model import compose_run, ComposeDescriptorBundle


def gen_stream(
    data_lst: tp.List[dict],
    metadata: dict,
    uid: str = None
) -> tp.Generator[tp.Tuple[str, dict], None, None]:
    """Generate a fake doc stream from data and metadata."""
    crb = compose_run(metadata=metadata, uid=uid if uid else str(uuid.uuid4()))
    yield "start", crb.start_doc
    if len(data_lst) == 0:
        yield "stop", crb.compose_stop()
    else:
        cdb: ComposeDescriptorBundle = crb.compose_descriptor(
            name="primary",
            data_keys=compose_data_keys(data_lst[0])
        )
        yield "descriptor", cdb.descriptor_doc
        for data in data_lst:
            yield "event", cdb.compose_event(data=data, timestamps=compose_timestamps(data))
        yield "stop", crb.compose_stop()


def compose_data_keys(data: tp.Dict[str, tp.Any]) -> tp.Dict[str, dict]:
    """Compose the data keys."""
    return {k: dict(**compose_data_info(v), source="PV:{}".format(k.upper())) for k, v in data.items()}


def compose_data_info(value: tp.Any) -> dict:
    """Compose the data information."""
    if isinstance(value, str):
        return {"dtype": "string", "shape": []}
    elif isinstance(value, float):
        return {"dtype": "number", "shape": []}
    elif isinstance(value, bool):
        return {"dtype": "boolean", "shape": []}
    elif isinstance(value, int):
        return {"dtype": "integer", "shape": []}
    else:
        return {"dtype": "array", "shape": np.shape(value)}


def compose_timestamps(data: tp.Dict[str, tp.Any]) -> tp.Dict[str, float]:
    """Compose the fake time for the data measurement."""
    return {k: time.time() for k in data.keys()}


def gen_stream_external(
    data_lst: tp.List[dict],
    metadata: dict,
    external_keys: tp.Set[str],
    uid: str = None
) -> tp.Generator[tp.Tuple[str, dict], None, None]:
    """Generate a fake doc stream with external (stream_resource/stream_datum) references.

    This simulates the document stream that would arrive over ZMQ from a detector
    that writes data to an external file and uses stream_resource/stream_datum.
    The external keys will have ``external: 'STREAM:'`` in the descriptor and
    unfilled references in the events.

    Parameters
    ----------
    data_lst : list of dict
        The data for each event. External keys should still contain the actual
        data values (used for computing data_keys shapes), but they will be
        replaced with datum uid references in the emitted events.
    metadata : dict
        Run metadata for the start document.
    external_keys : set of str
        Which data keys should be treated as external (stream_resource/stream_datum).
    uid : str, optional
        UID for the run. Generated if not provided.
    """
    run_uid = uid if uid else str(uuid.uuid4())
    crb = compose_run(metadata=metadata, uid=run_uid)
    yield "start", crb.start_doc
    if len(data_lst) == 0:
        yield "stop", crb.compose_stop()
        return

    # Build data_keys, marking external keys
    data_keys = {}
    for k, v in data_lst[0].items():
        info = compose_data_info(v)
        info["source"] = "PV:{}".format(k.upper())
        if k in external_keys:
            info["external"] = "STREAM:"
        data_keys[k] = info

    cdb: ComposeDescriptorBundle = crb.compose_descriptor(
        name="primary",
        data_keys=data_keys,
    )
    yield "descriptor", cdb.descriptor_doc
    desc_uid = cdb.descriptor_doc["uid"]

    # Emit stream_resource and stream_datum for each external key
    sr_bundles = {}
    for key in external_keys:
        sr_bundle = crb.compose_stream_resource(
            mimetype="application/x-hdf5",
            uri="file:///tmp/fake_{}.h5".format(key),
            data_key=key,
            parameters={"dataset": ["entry", "data", key]},
        )
        sr_bundles[key] = sr_bundle
        yield "stream_resource", sr_bundle.stream_resource_doc

    for i, data in enumerate(data_lst):
        # Emit stream_datum for each external key
        for key in external_keys:
            sd_doc = sr_bundles[key].compose_stream_datum(
                seq_nums={"start": i, "stop": i + 1},
                indices={"start": i, "stop": i + 1},
            )
            sd_doc["descriptor"] = desc_uid
            yield "stream_datum", sd_doc

        # Emit event with unfilled external keys
        event_data = {}
        event_ts = {}
        event_filled = {}
        for k, v in data.items():
            if k in external_keys:
                event_data[k] = "unfilled_datum_ref"
                event_filled[k] = False
            else:
                event_data[k] = v
                event_filled[k] = True
            event_ts[k] = time.time()

        yield "event", cdb.compose_event(
            data=event_data,
            timestamps=event_ts,
            filled=event_filled,
        )

    yield "stop", crb.compose_stop()
