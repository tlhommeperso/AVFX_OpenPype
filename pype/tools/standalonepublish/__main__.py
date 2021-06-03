import os
import sys
import ctypes
import signal
import app
from Qt import QtWidgets, QtGui
from avalon import style
from pype.api import resources
from pype.tools.standalonepublish.widgets.constants import HOST_NAME


if __name__ == "__main__":
    os.environ["AVALON_APP"] = HOST_NAME

    # Allow to change icon of running process in windows taskbar
    if os.name == "nt":
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(
            u"standalonepublish"
        )

    qt_app = QtWidgets.QApplication([])
    # app.setQuitOnLastWindowClosed(False)
    qt_app.setStyleSheet(style.load_stylesheet())
    icon = QtGui.QIcon(resources.pype_icon_filepath())
    qt_app.setWindowIcon(icon)

    def signal_handler(sig, frame):
        print("You pressed Ctrl+C. Process ended.")
        qt_app.quit()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    window = app.Window(sys.argv[-1].split(os.pathsep))
    window.show()

    sys.exit(qt_app.exec_())