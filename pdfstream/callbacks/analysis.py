import copy
import datetime
import typing
import typing as tp
from configparser import ConfigParser
from pathlib import Path

import databroker.mongo_normalized
import event_model
import matplotlib.pyplot as plt
import numpy as np
from bluesky.callbacks.stream import LiveDispatcher
from databroker.v1 import Broker
from event_model import RunRouter
from pyFAI.azimuthalIntegrator import AzimuthalIntegrator
from suitcase.csv import Serializer as CSVSerializer
from suitcase.json_metadata import Serializer as JsonSerializer
import pandas
from tiled.client import from_uri
from tiled.client.array import ArrayClient
from tiled.client.dataframe import DataFrameClient

import pdfstream
import pdfstream.callbacks.from_descriptor as from_desc
import pdfstream.callbacks.from_event as from_event
import pdfstream.callbacks.from_start as from_start
import pdfstream.integration.tools as integ
import pdfstream.io as io
from pdfstream.callbacks.basic import MyLiveImage, LiveMaskedImage, LiveWaterfall, StackedNumpyTextExporter, \
    SmartScalarPlot, MyTiffSerializer, CalibrationExporter, NumpyExporter, YamlSerializer
from pdfstream.errors import ValueNotFoundError
from pdfstream.units import LABELS
from pdfstream.vend.formatters import SpecialStr

try:
    from diffpy.pdfgetx import PDFConfig, PDFGetter

    _PDFGETX_AVAILABLE = True
except ImportError:
    _PDFGETX_AVAILABLE = False


class BasicAnalysisConfig(ConfigParser):
    """The basic configuration that is shared by analysis and calibration."""

    @property
    def raw_db(self) -> str:
        return self.get("DATABASE", "raw_db", fallback="")

    @property
    def dark_identifier(self):
        return self.get("METADATA", "dk_identifier", fallback="dark_frame")

    @property
    def dk_id_key(self):
        return self.get("METADATA", "dk_id_key", fallback="sc_dk_field_uid")

    @property
    def composition_key(self):
        return self.get("METADATA", "composition_key", fallback="sample_composition")

    @property
    def wavelength_key(self):
        return self.get("METADATA", "wavelength_key", fallback="bt_wavelength")

    def to_dict(self):
        """Convert the configuration to a dictionary."""
        return {s: dict(self.items(s)) for s in self.sections()}

    def read_user_config(self, user_config):
        self.read_dict({"ANALYSIS": user_config})


class AnalysisConfig(BasicAnalysisConfig):
    """The configuration for analysis callbacks."""

    def __init__(self, *args, **kwargs):
        super(AnalysisConfig, self).__init__(*args, **kwargs)
        self.add_section("DATABASE")
        self.add_section("METADATA")
        self.add_section("ANALYSIS")

    @property
    def calibration_md_key(self):
        return self.get("METADATA", "calibration_md_key", fallback="calibration_md")

    @property
    def sample_name_key(self):
        return self.get("METADATA", "sample_name_key", fallback="sample_name")

    @property
    def auto_mask(self):
        return self.getboolean("ANALYSIS", "auto_mask", fallback=True)

    @property
    def user_mask(self):
        mask_file = self.get("ANALYSIS", "user_mask", fallback="")
        if not mask_file:
            return None
        return io.load_matrix_flexible(mask_file)

    @property
    def mask_setting(self):
        return {
            "alpha": self.getfloat("ANALYSIS", "alpha", fallback=2.5),
            "edge": self.getint("ANALYSIS", "edge", fallback=20),
            "lower_thresh": self.getfloat("ANALYSIS", "lower_thresh", fallback=0.),
            "upper_thresh": self.getfloat("ANALYSIS", "upper_thresh", fallback=None)
        }

    @property
    def integ_setting(self):
        return {
            "npt": self.getint("ANALYSIS", "npt", fallback=2048),
            "correctSolidAngle": self.getboolean("ANALYSIS", "correctSolidAngle", fallback=False),
            "polarization_factor": self.getfloat("ANALYSIS", "polarization_factor", fallback=0.99),
            "method": self.get("ANALYSIS", "method", fallback="bbox,csr,cython"),
            "normalization_factor": self.getfloat("ANALYSIS", "normalization_factor", fallback=1.),
            "unit": "2th_deg"
        }

    @property
    def trans_setting(self):
        return {
            "rpoly": self.getfloat("ANALYSIS", "rpoly", fallback=1.2),
            "qmaxinst": self.getfloat("ANALYSIS", "qmaxinst", fallback=24.),
            "qmin": self.getfloat("ANALYSIS", "qmin", fallback=0.),
            "qmax": self.getfloat("ANALYSIS", "qmax", fallback=22.),
            "rmin": self.getfloat("ANALYSIS", "rmin", fallback=0.),
            "rmax": self.getfloat("ANALYSIS", "rmax", fallback=30.),
            "rstep": self.getfloat("ANALYSIS", "rstep", fallback=0.01),
            "dataformat": "QA"
        }

    @property
    def directory(self):
        return self.get("ANALYSIS", "tiff_base", fallback=".")

    @property
    def file_prefix(self):
        return SpecialStr(
            self.get("ANALYSIS", "file_prefix", fallback="start[uid]_"))

    @property
    def valid_keys(self):
        res = set(self.get("ANALYSIS", "valid_keys", fallback="").replace(" ", "").split(","))
        if "" in res:
            res.remove("")
        return res

    @property
    def save_file(self):
        return self.getboolean("ANALYSIS", "save_file", fallback=True)

    @property
    def pdfgetx(self):
        return self.getboolean("ANALYSIS", "pdfgetx", fallback=True)


class AnalysisStream(LiveDispatcher):
    """The secondary stream for data analysis.

    It inject the configuration into start document and emit processed data to the subscribers.
    """

    def __init__(self, config: AnalysisConfig):
        super(AnalysisStream, self).__init__()
        self.init_config = config
        self.config: typing.Union[AnalysisConfig, None] = None
        db_name = config.raw_db
        self.db = Broker.named(db_name) if db_name else None
        self.valid_keys = config.valid_keys
        self.start_doc = {}
        self.ai = None
        self.bt_info = {}
        self.image_key = ""
        self.dark_image = None
        self.indeps = None
        self.indep2unit = None
        self.dirc = None
        self.file_prefix = None

        self._tiled_client = from_uri("https://tiled.nsls2.bnl.gov/api/v1/metadata/xpd/sandbox")

    def start(self, doc, _md=None):
        io.server_message("Receive the start of '{}'.".format(doc["uid"]))
        self.clear_cache()
        # get indeps
        self.indeps = from_start.get_indeps(doc, exclude={"time"})
        # copy the default config and read the user config
        self.config = copy.deepcopy(self.init_config)
        self.config.read_user_config(doc.get("user_config", {}))
        # record start doc for later use
        self.start_doc = doc
        # find ai
        try:
            self.ai = from_start.query_ai(
                doc,
                calibration_md_key=self.config.calibration_md_key
            )
            io.server_message("Read the calibration data in the start.")
        except ValueNotFoundError as error:
            self.ai = None
            io.server_message("Failed to find calibration data: {}.".format(str(error)))
        # find bt info
        try:
            self.bt_info = from_start.query_bt_info(
                doc,
                composition_key=self.config.composition_key,
                wavelength_key=self.config.wavelength_key,
                default_composition="Ni"
            )
        except ValueNotFoundError as error:
            self.bt_info = {}
            io.server_message("Encounter error when searching the metadata: {}.".format(str(error)))
        # create new start
        new_start = dict(**doc, an_config=self.config.to_dict(), pdfstream_version=pdfstream.__version__)
        # inject readable time
        new_start["readable_time"] = datetime.datetime.fromtimestamp(doc["time"]).strftime(
            "%Y%m%d-%H%M%S")
        # add sample_name and if it is not there
        new_start.setdefault("sample_name", "unnamed_sample")
        new_start.setdefault("original_run_uid", doc["uid"])
        # create directoy
        d = self.config.directory
        fp = self.config.file_prefix
        self.dirc = Path(str(Path(d).expanduser().joinpath(new_start["sample_name"])).format(start=doc))
        if self.config.save_file:
            self.dirc.mkdir(parents=True, exist_ok=True)
        # create file prefix
        self.file_prefix = fp.format(start=new_start)
        return super(AnalysisStream, self).start(new_start)

    def event_page(self, doc):
        for event_doc in event_model.unpack_event_page(doc):
            self.event(event_doc)

    def descriptor(self, doc):
        self.indep2unit = from_desc.get_units(doc["data_keys"], self.indeps)
        self.image_key = from_desc.find_one_image(doc)
        io.server_message("Find the data key '{}' in the stream '{}'.".format(self.image_key, doc["name"]))
        try:
            self.dark_image = from_start.query_dk_img(
                self.start_doc,
                db=self.db,
                dk_id_key=self.config.dk_id_key,
                det_name=self.image_key
            )
        except ValueNotFoundError as error:
            self.dark_image = None
            io.server_message("Failed to find the dark: " + str(error))
        return super(AnalysisStream, self).descriptor(doc)

    def event(self, doc, _md=None):
        io.server_message("Start processing the event {}.".format(doc["seq_num"]))
        data = self.process_data(doc)
        io.server_message("Finish processing the event {}.".format(doc["seq_num"]))
        return self.process_event(dict(data=data, descriptor=doc["descriptor"]))

    def stop(self, doc, _md=None):
        io.server_message("Receive the stop of '{}'.".format(doc["run_start"]))
        return super(AnalysisStream, self).stop(doc)

    def _get_uid_from_uri(self, uri) -> str:
        return uri.split("/")[-1]

    def _write_dataframe_to_tiled(self, data_dict, columns, group_key, default_md):
        df_data = []
        for key in columns:
            df_data.append(data_dict[key])
        df_data = np.array(df_data).T

        df = pandas.DataFrame(df_data, columns=columns)

        metadata = {
            "field": group_key,
            **default_md
        }
        for m in ["argmax", "max"]:
            k = f"{group_key}_{m}"
            if k in data_dict:
                metadata[k] = data_dict[k]

        entry = self._tiled_client.write_dataframe(df, metadata=metadata)
        try:
            print(f"f{entry.key = }")
        except Exception:
            print("The 'key' attribute is not found in the entry.")
        entry_uri = entry.uri
        entry_uid = self._get_uid_from_uri(entry_uri)

        # print(f"{group_key = }  {entry_uri = }  {entry_uid = }")

        return {"uri": entry_uri, "uid": entry_uid}

    def process_data(self, doc) -> dict:
        """Process the data in the event doc. Return a dictionary of processed data."""
        # the raw image in the data
        raw_img = from_event.get_image_from_event(doc, det_name=self.image_key)
        # copy the data
        raw_data = {k: copy.copy(v) for k, v in doc["data"].items() if k != self.image_key}
        # get filename
        indep_str = from_desc.get_indep_str(doc["data"], self.indep2unit)
        filename = self.file_prefix + indep_str + "{:04d}".format(doc["seq_num"])
        directory = str(self.dirc)
        if not self.config.save_file:
            filename, directory = None, None
        # process the data output a dictionary
        an_data = process(
            raw_img=raw_img,
            ai=self.ai,
            user_mask=self.config.user_mask,
            auto_mask=self.config.auto_mask,
            dk_img=self.dark_image,
            integ_setting=self.config.integ_setting,
            mask_setting=self.config.mask_setting,
            pdfgetx_setting=dict(
                **self.bt_info,
                **self.config.trans_setting
            ),
            filename=filename,
            directory=directory,
            pdfgetx=self.config.pdfgetx
        )
        # filter the data
        if self.valid_keys:
            an_data = self.filter(an_data)

        from pprint import pformat
        print(f"{self.__class__.__name__}:\ndoc={pformat(doc)}\nraw_data={pformat(raw_data)}\nan_data={pformat(an_data)}")

        # Enter the information to Tiled:
        tiled_dict = {}
        default_md = {"run_start": self.start_doc["uid"]}

        # DataFrames:

        # DataFrame: chi
        group_key = "chi"
        tiled_key = f"tiled_{group_key}"
        columns=["chi_2theta", "chi_Q", "chi_I"]
        tiled_dict[tiled_key] = self._write_dataframe_to_tiled(
            an_data,
            columns=columns,
            group_key=group_key,
            default_md=default_md,
        )
        for key in columns + ["iq_I", "iq_Q"]:  # special case
            an_data[key] = tiled_dict[tiled_key]["uid"]

        # DataFrame: fq/sq
        group_key = "fqsq"
        tiled_key = f"tiled_{group_key}"
        columns = ["fq_F", "fq_Q", "sq_Q", "sq_S"]
        tiled_dict[tiled_key] = self._write_dataframe_to_tiled(
            an_data,
            columns=columns,
            group_key=group_key,
            default_md=default_md,
        )
        for key in columns:
            an_data[key] = tiled_dict[tiled_key]["uid"]

        # DataFrame: gr
        group_key = "gr"
        tiled_key = f"tiled_{group_key}"
        columns = ["gr_G", "gr_r"]
        tiled_dict[tiled_key] = self._write_dataframe_to_tiled(
            an_data,
            columns=columns,
            group_key=group_key,
            default_md=default_md,
        )
        for key in columns:
            an_data[key] = tiled_dict[tiled_key]["uid"]

        # Arrays:
        for key in ["dk_sub_image", "mask"]:
            tiled_key = f"tiled_{key}"
            entry = self._tiled_client.write_array(
                an_data[key],
                metadata={"field": key, **default_md})
            entry_uri = entry.uri
            entry_uid = entry_uri.split("/")[-1]
            tiled_dict[tiled_key] = {"uri": entry_uri, "uid": entry_uid}
            an_data[key] = tiled_dict[tiled_key]["uid"]

        print(f"tiled_dict:\n{pformat(tiled_dict)}")
        print(f"an_data:\n{pformat(an_data)}")

        # the final output data is a combination of the independent variables and processed data
        return dict(**raw_data, **an_data, **tiled_dict)

    def filter(self, data: dict):
        return {k: v for k, v in data.items() if k in self.valid_keys}

    def clear_cache(self):
        """Clear the cache."""
        self.config = None
        self.start_doc = {}
        self.ai = None
        self.bt_info = {}
        self.image_key = ""
        self.dark_image = None


def write_out_pdfgetter(pdfgetter: PDFGetter, dirc: str, filename: str):
    n = 3
    names = ["sq", "fq", "gr"]
    folders = ["sq", "fq", "pdf"]
    suffixes = [".sq", ".fq", ".gr"]
    for i in range(n):
        out_dir = Path(dirc).joinpath(folders[i])
        out_dir.mkdir(exist_ok=True)
        out_file = out_dir.joinpath(filename).with_suffix(suffixes[i])
        pdfgetter.writeOutput(str(out_file), names[i])
    return


def process(
        *,
        raw_img: np.ndarray,
        ai: tp.Union[None, AzimuthalIntegrator],
        user_mask: np.ndarray = None,
        auto_mask: bool = True,
        dk_img: np.ndarray = None,
        integ_setting: dict = None,
        mask_setting: dict = None,
        pdfgetx_setting: dict = None,
        filename: str = None,
        directory: str = None,
        pdfgetx: bool = True
) -> dict:
    """The function to process the data from event."""
    # initialize the data dictionary
    data = {
        "dk_sub_image": raw_img,
        "mask": np.zeros_like(raw_img),
        "chi_2theta": np.array([0.]),
        "chi_Q": np.array([0.]),
        "chi_I": np.array([0.]),
        "chi_max": np.float64(0.),
        "chi_argmax": np.float64(0.),
        "iq_Q": np.array([0.]),
        "iq_I": np.array([0.]),
        "sq_Q": np.array([0.]),
        "sq_S": np.array([0.]),
        "fq_Q": np.array([0.]),
        "fq_F": np.array([0.]),
        "gr_r": np.array([0.]),
        "gr_G": np.array([0.]),
        "gr_max": np.float64(0.),
        "gr_argmax": np.float64(0.)
    }
    # dark subtraction
    if dk_img is not None:
        data["dk_sub_image"] = np.subtract(raw_img, dk_img)
    # if no calibration, output data now
    if ai is None:
        return data
    # do auto masking if specified
    if auto_mask:
        data["mask"], _ = integ.auto_mask(data["dk_sub_image"], ai, mask_setting=mask_setting, user_mask=user_mask)
    elif user_mask is not None:
        data["mask"] = user_mask
    # integration
    tth, y = ai.integrate1d(data["dk_sub_image"], mask=data["mask"], **integ_setting)
    x = 4. * np.pi / (ai.get_wavelength() * 1e10) * np.sin(np.deg2rad(tth / 2.))
    chi_max_ind = np.argmax(y)
    data.update(
        {
            "chi_2theta": tth, "chi_Q": x, "chi_I": y, "chi_max": x[chi_max_ind], "chi_argmax": y[chi_max_ind]
        }
    )
    # transformation
    if not pdfgetx:
        return data
    if not _PDFGETX_AVAILABLE:
        io.server_message("diffpy.pdfgetx is not installed. No use [0.] for all the relevant data.")
        return data
    pdfconfig = PDFConfig(**pdfgetx_setting)
    pdfgetter = PDFGetter(pdfconfig)
    pdfgetter(x, y)
    iq, sq, fq, gr = [x, y], pdfgetter.sq, pdfgetter.fq, pdfgetter.gr
    gr_max_ind = np.argmax(gr[1])
    data.update(
        {
            "iq_Q": iq[0], "iq_I": iq[1], "sq_Q": sq[0], "sq_S": sq[1], "fq_Q": fq[0], "fq_F": fq[1],
            "gr_r": gr[0], "gr_G": gr[1], "gr_max": gr[1][gr_max_ind], "gr_argmax": gr[0][gr_max_ind]
        }
    )
    if filename and directory:
        io.server_message("Save the PDF data in '{}' using file name '{}'.".format(directory, filename))
        write_out_pdfgetter(pdfgetter, directory, filename)
    return data


class ExportConfig(ConfigParser):
    """The configuration of exporter."""

    def __init__(self, *args, **kwargs):
        super(ExportConfig, self).__init__(*args, **kwargs)
        self.add_section("SUITCASE")

    @property
    def calib_identifier(self):
        return self.get("METADATA", "calib_identifier", fallback="is_calibration")

    def get_exports(self):
        return set(
            self.get("SUITCASE", "exports", fallback="poni,tiff,mask,yaml,csv,txt").replace(" ", "").split(","))

    def get_file_prefix(self):
        return SpecialStr(
            self.get("SUITCASE", "file_prefix", fallback="{start[original_run_uid]}_{start[readable_time]}_"))

    @property
    def directory_template(self):
        return SpecialStr(self.get("SUITCASE", "directory_template", fallback="{start[sample_name]}_data"))

    @property
    def tiff_base(self):
        """Settings for the base folder."""
        dir_path = self.get("SUITCASE", "tiff_base")
        if not dir_path:
            dir_path = "~/pdfstream_data"
            io.server_message("Missing tiff_base in configuration. Use '{}'".format(dir_path))
        path = Path(dir_path).expanduser()
        return path

    @tiff_base.setter
    def tiff_base(self, value: str):
        self.set("SUITCASE", "tiff_base", value)

    @property
    def tiff_setting(self):
        return {
            "astype": self.get("SUITCASE", "tiff_astype", fallback="float32"),
            "bigtiff": self.getboolean("SUITCASE", "tiff_bigtiff", fallback=False),
            "byteorder": self.get("SUITCASE", "tiff_byteorder", fallback=None),
            "imagej": self.get("SUITCASE", "tiff_imagej", fallback=False)
        }


class TiledClientTypeException(Exception):
    ...

def fill_data_from_tiled(data, tiled_client):
    """The helper function to fill the event's data field with the data from sandbox Tiled.

    Parameters
    ----------
        data (dict): a subset of the event document via the 'data' key).
        tiled_client (tiled.client...): a Tiled client instance.

    Examples
    --------

    Queueries can look like that:

        In [69]: queries
        Out[69]:
        {'32cf499e-0b28-4806-a83f-111f993812e6': <ArrayClient shape=(2048, 2048) chunks=((2048,), (2048,)) dtype=float64>,
        'fef412dd-88a3-48eb-8c12-93883a7983ea': <DataFrameClient ['gr_G', 'gr_r']>,
        '9c1470af-dbd3-40f6-bea8-f5058e6bb146': <ArrayClient shape=(2048, 2048) chunks=((2048,), (2048,)) dtype=float64>,
        '41ad8fdf-12d0-4c1a-b057-87032ba6f34b': <DataFrameClient ['fq_F', 'fq_Q', 'sq_Q', 'sq_S']>,
        '6e2eedec-2a03-421a-96e4-59f77a27213c': <DataFrameClient ['chi_2theta', 'chi_Q', 'chi_I']>}
    """
    all_values = list(data.values())
    all_uids = [x for x in all_values if type(x) is str]
    queries = {}

    for uid in set(all_uids):
        queries[uid] = tiled_client[uid]

    for key in data:
        if key.startswith("tiled_"):  # the values are dictionaries (can't be used as keys)
            continue
        client = queries.get(data[key], None)  # check if the uid (=data[key]) is in the 'queries' keys, otherwise skip the filling.
        if client is None:
            continue
        if isinstance(client, ArrayClient):  # image data
            data[key] = client.read()
        elif isinstance(client, DataFrameClient):  # Pandas DataFrames
            new_key = key
            if key in ["iq_I", "iq_Q"]:  # special case
                new_key = key.replace("iq", "chi")
            data[key] = np.array(client.read()[new_key])
        else:
            raise TiledClientTypeException(f"Unknown tiled client type: {type(client)}")

    return data


class Exporter(RunRouter):
    """Export the processed data to file systems. Add readable_time to start doc."""

    def __init__(self, config: ExportConfig):
        self._config = config
        factory = ExporterFactory(config)
        super().__init__([factory], handler_registry=databroker.mongo_normalized.discover_handlers())
        io.server_message("Data will be exported in '{}' in a proposal directory.".format(str(config.tiff_base)))

        self._tiled_client = from_uri("https://tiled.nsls2.bnl.gov/api/v1/metadata/xpd/sandbox")

    def start(self, start_doc):
        save_dir = self._config.tiff_base.joinpath(self._config.directory_template.format(start=start_doc))
        io.server_message("Data will be exported in '{}'.".format(save_dir))
        io.server_message("Receive the start of '{}'.".format(start_doc["uid"]))
        return super(Exporter, self).start(start_doc)

    def descriptor(self, doc):
        doc["data_keys"]["dk_sub_image"].update({"dtype": "array", "shape": [-1, -1]})
        super().descriptor(doc)

    def event(self, doc):
        from pprint import pformat
        print(f"{self.__class__.__name__} (before filling from Tiled): {pformat(doc)}")

        data = doc["data"]
        # Get information for all fillable entries in 'an_data' dict from Tiled:
        data = fill_data_from_tiled(data=data, tiled_client=self._tiled_client)

        print(f"{self.__class__.__name__} (after filling from Tiled): {pformat(doc)}")

        io.server_message("Export data in the event {}.".format(doc["seq_num"]))
        return super(Exporter, self).event(doc)

    def stop(self, doc):
        io.server_message("Finish exporting data for '{}'.".format(doc["run_start"]))
        return super(Exporter, self).stop(doc)


class ExporterFactory:
    """The factory for the exporter run router."""

    def __init__(self, config: ExportConfig):
        self.config = config

    def __call__(self, name: str, doc: dict) -> tp.Tuple[list, list]:
        if name != "start":
            return [], []
        data_folder = self.config.tiff_base.joinpath(self.config.directory_template.format(start=doc))
        data_folder.mkdir(exist_ok=True, parents=True)
        callbacks = []
        exports = self.config.get_exports()
        file_prefix = self.config.get_file_prefix()
        if "tiff" in exports:
            cb = MyTiffSerializer(
                str(data_folder.joinpath("dark_sub")),
                file_prefix=file_prefix,
                data_keys=["dk_sub_image"],
                **self.config.tiff_setting
            )
            callbacks.append(cb)
        if "mask" in exports:
            cb = NumpyExporter(
                str(data_folder.joinpath("mask")),
                file_prefix=file_prefix,
                data_keys=["mask"]
            )
            callbacks.append(cb)
        if "json" in exports:
            cb = JsonSerializer(
                str(data_folder.joinpath("meta")),
                file_prefix=file_prefix
            )
            callbacks.append(cb)
        if "yaml" in exports:
            cb = YamlSerializer(
                str(data_folder.joinpath("meta")),
                file_prefix=file_prefix
            )
            callbacks.append(cb)
        if "csv" in exports:
            cb = CSVSerializer(
                str(data_folder.joinpath("scalar_data")),
                file_prefix=file_prefix
            )
            callbacks.append(cb)
        if "txt" in exports:
            cb = StackedNumpyTextExporter(
                file_prefix,
                str(data_folder.joinpath("integration")), ("chi_2theta", "chi_I"), "_mean_tth.chi",
                str(data_folder.joinpath("integration")), ("chi_Q", "chi_I"), "_mean_q.chi"
            )
            callbacks.append(cb)
        if "poni" in exports and doc.get(self.config.calib_identifier, False):
            cb = CalibrationExporter(str(data_folder.joinpath("calib")), file_prefix=file_prefix)
            callbacks.append(cb)
        return callbacks, []


def _get_vlim(image: np.ndarray) -> tp.Tuple[float, float]:
    """Get the vmin and vmax from a image."""
    m = np.mean(image)
    std = np.std(image)
    return max(m - 2 * std, 0), m + 2 * std


class VisConfig(ConfigParser):
    """The configuration of visualization."""

    def __init__(self, *args, **kwargs):
        super(VisConfig, self).__init__(*args, **kwargs)
        self.add_section("VISUALIZATION")

    def get_visualizers(self):
        return set(
            self.get(
                "VISUALIZATION",
                "visualizers",
                fallback="dk_sub_image,masked_image,chi,chi_2theta,iq,sq,fq,gr,chi_max,chi_argmax,gr_max,gr_argmax"
            ).replace(" ", "").split(",")
        )

    @property
    def vis_masked_image(self):
        return {
            "cmap": "viridis",
            "limit_func": _get_vlim
        }

    @property
    def vis_dk_sub_image(self):
        return {
            "cmap": "viridis",
            "limit_func": _get_vlim
        }

    @property
    def vis_2theta(self):
        return {
            "xlabel": LABELS.tth[0],
            "ylabel": LABELS.tth[1]
        }

    @property
    def vis_chi(self):
        return {
            "xlabel": LABELS.chi[0],
            "ylabel": LABELS.chi[1]
        }

    @property
    def vis_iq(self):
        return {
            "xlabel": LABELS.iq[0],
            "ylabel": LABELS.iq[1]
        }

    @property
    def vis_sq(self):
        return {
            "xlabel": LABELS.sq[0],
            "ylabel": LABELS.sq[1]
        }

    @property
    def vis_fq(self):
        return {
            "xlabel": LABELS.fq[0],
            "ylabel": LABELS.fq[1]
        }

    @property
    def vis_gr(self):
        return {
            "xlabel": LABELS.gr[0],
            "ylabel": LABELS.gr[1]
        }


class Visualizer(RunRouter):
    """Visualize the analyzed data. It can be subscribed to a live dispatcher."""

    def __init__(self, config: VisConfig):
        self._factory = VisFactory(config)
        super().__init__([self._factory])

        self._tiled_client = from_uri("https://tiled.nsls2.bnl.gov/api/v1/metadata/xpd/sandbox")
        print(f"{self._tiled_client = }")

    def event(self, doc):
        from pprint import pformat
        io.server_message(f"{self.__class__.__name__} (before filling from Tiled): {pformat(doc)}")

        data = doc["data"]
        # Get information for all fillable entries in 'an_data' dict from Tiled:
        data = fill_data_from_tiled(data=data, tiled_client=self._tiled_client)

        io.server_message(f"{self.__class__.__name__} (after filling from Tiled): {pformat(doc)}")

        return super().event(doc)

    def show_figs(self):
        """Show all the figures in the callbacks in the factory."""
        return self._factory.show()


class VisFactory:
    """The factory of visualization callbacks."""

    def __init__(self, config: VisConfig):
        self.config = config
        self.cb_lst = []
        visualizers = self.config.get_visualizers()
        # images
        if "dk_sub_image" in visualizers:
            self.cb_lst.append(
                MyLiveImage("dk_sub_image", **self.config.vis_dk_sub_image)
            )
        if "masked_image" in visualizers:
            self.cb_lst.append(
                LiveMaskedImage("dk_sub_image", "mask", **self.config.vis_masked_image)
            )
        # one dimensional array waterfall
        for xfield, yfield, tag, vis_config in [
            ("chi_2theta", "chi_I", "chi_2theta", self.config.vis_2theta),
            ("chi_Q", "chi_I", "chi", self.config.vis_chi),
            ("iq_Q", "iq_I", "iq", self.config.vis_iq),
            ("sq_Q", "sq_S", "sq", self.config.vis_sq),
            ("fq_Q", "fq_F", "fq", self.config.vis_fq),
            ("gr_r", "gr_G", "gr", self.config.vis_gr)
        ]:
            if tag in visualizers:
                fig = plt.figure()
                self.cb_lst.append(
                    LiveWaterfall(xfield, yfield, ax=fig.add_subplot(111), **vis_config)
                )
                fig.show()
        for field in ("gr_max", "gr_argmax", "chi_max", "chi_argmax"):
            if field in visualizers:
                fig = plt.figure()
                ax = fig.add_subplot(111)
                self.cb_lst.append(
                    SmartScalarPlot(field, ax=ax, marker="o")
                )
                fig.show()

    def __call__(self, name: str, doc: dict) -> tp.Tuple[list, list]:
        if name != "start":
            return [], []
        return self.cb_lst, []

    def show(self):
        for cb in self.cb_lst:
            try:
                cb.show()
            except AttributeError:
                pass
        return
