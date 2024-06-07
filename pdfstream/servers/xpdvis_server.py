from bluesky.callbacks.best_effort import BestEffortCallback

from pdfstream.callbacks.analysis import Visualizer, VisConfig
from pdfstream.servers.base import BaseServer as BaseServerZMQ, BaseServerKafkaViz, BaseServerKafkaAnalysis, ServerConfig
import matplotlib.pyplot as plt


class XPDVisServerConfig(ServerConfig, VisConfig):
    """A configuration for the XPDVisServer."""
    pass


#BaseServerClass = BaseServerZMQ
# BaseServerClass = BaseServerKafkaViz
BaseServerClass = BaseServerKafkaAnalysis


class XPDVisServer(BaseServerClass):
    """A server that visualizes the analyzed data from the xpd server."""

    def __init__(self, config: XPDVisServerConfig):
        super(XPDVisServer, self).__init__(config)
        bec = BestEffortCallback()
        bec.disable_plots()
        self.subscribe(bec)
        self.subscribe(Visualizer(config))


def make_and_run(
    cfg_file: str = None,
    *,
    suppress_warning: bool = True,
    test_mode: bool = False
):
    """Run the xpd data reduction server.

    The server will receive message from proxy and process the data in the message. The processed data will be
    visualized and exported to database and the file system.

    Parameters
    ----------
    cfg_file :
        The path to configuration .ini file. The default path is "~/.config/acq/xpd_server.ini".

    suppress_warning :
        If True, all warning will be suppressed. Turn it to False when running in a test.

    test_mode :
        If True, just create a server but not start. Used for test.
    """
    if suppress_warning:
        import warnings
        warnings.simplefilter("ignore")
    config = XPDVisServerConfig()
    config.read(cfg_file)
    server = XPDVisServer(config)
    if not test_mode:
        def f():
            plt.gcf().canvas.draw_idle()
            plt.gcf().canvas.start_event_loop(0.05)
        server.install_qt_kicker()
        #server.start()   #ZMQ
        #server.start(work_during_wait=lambda: plt.pause(0.05))
        server.start(work_during_wait=f)   #kafka
