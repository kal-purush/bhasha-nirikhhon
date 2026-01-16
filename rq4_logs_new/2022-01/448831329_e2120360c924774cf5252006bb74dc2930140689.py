try:
    import cv2
    import numpy as np
    import sortedcontainers
    import PyQt5
    import qdarkstyle
except ModuleNotFoundError:
    __import__('pip').main(['install', 'sortedcontainers', 'numpy', 'opencv_python', 'PyQt5', 'qdarkstyle'])

import argparse
import bisect
import os
import sys
import threading
import time
import datetime	
from collections import OrderedDict

import cv2
import numpy as np
from PyQt5 import QtCore, QtWidgets
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
from sortedcontainers import SortedSet, SortedDict

# Tuan
import pandas as pd

class QTableViewer(QTableWidget):
    def __init__(self, data, *args):
        QTableWidget.__init__(self, *args)
        self.data = data
        self.setData()
        self.resizeColumnsToContents()
        self.resizeRowsToContents()
 
    def setData(self): 
        horHeaders = []
        for n, key in enumerate(sorted(self.data.keys())):
            horHeaders.append(key)
            for m, item in enumerate(self.data[key]):
                newitem = QTableWidgetItem(item)
                self.setItem(m, n, newitem)
        self.setHorizontalHeaderLabels(horHeaders)


class QUpdater(QThread):
    update = pyqtSignal()

    def __init__(self, interval=0):
        super(QUpdater, self).__init__()
        self._running = True
        self.paused = False
        self.wasPaused = self.paused
        self.interval = interval
        self.sem = threading.BoundedSemaphore()
        self.pause()

    def run(self):
        while self._running:
            if self.paused:
                self.sem.acquire()
            self.update.emit()
            if self.interval > 0:
                time.sleep(self.interval)

    def toggle(self):
        if self.paused:
            self.unpause()
        else:
            self.pause()

    def pause(self):
        self.sem.acquire(blocking=False)
        self.paused = True

    def unpause(self):
        self.sem.release()
        self.paused = False

    def block(self):
        self.wasPaused = self.paused
        self.pause()

    def unblock(self):
        if not self.wasPaused:
            self.unpause()

    def stop(self):
        self._running = False

    def setInterval(self, interval):
        self.interval = interval

    def setFps(self, fps):
        self.interval = 1 / fps


class QEventLabel(QLabel):
    mouseEntered = pyqtSignal()
    mouseLeft = pyqtSignal()
    textChanged = pyqtSignal(str)

    def __init__(self, text=None):
        QLabel.__init__(self, text)
        self.default = text
        self.mouseOver = False

    def enterEvent(self, ev):
        self.mouseOver = True
        self.mouseEntered.emit()

    def leaveEvent(self, ev):
        self.mouseOver = False
        self.mouseLeft.emit()

    def setText(self, a0):
        super(QEventLabel, self).setText(a0)
        self.textChanged.emit(a0)

    def clear(self):
        super(QEventLabel, self).clear()
        self.textChanged.emit("")


class QOpenCVLabel(QLabel):
    clicked = pyqtSignal(int, int)
    hovered = pyqtSignal(int, int)
    dragged = pyqtSignal(int, int)
    released = pyqtSignal(int, int)

    def __init__(self, parent=None):
        QLabel.__init__(self, parent)
        self.setMouseTracking(True)
        self.setMinimumSize(1, 1)
        self.pixmap = None
        self._lock = threading.Lock()

    def setOpenCVImage(self, cvImage):
        with self._lock:
            self.pixmap = QPixmap.fromImage(QImage(cvImage.data, cvImage.shape[1], cvImage.shape[0],
                                                   QImage.Format_RGB888).rgbSwapped())
            self.setPixmap(self.pixmap.scaled(self.size()))

    def resizeEvent(self, ev):
        if self.pixmap is not None:
            with self._lock:
                self.setPixmap(self.pixmap.scaled(self.size()))

    def mousePressEvent(self, ev):
        if ev.button() == Qt.LeftButton:
            pos = ev.pos()
            self.clicked.emit(pos.x(), pos.y())

    def mouseMoveEvent(self, ev):
        pos = ev.pos()
        if ev.buttons() == Qt.LeftButton:
            self.dragged.emit(pos.x(), pos.y())
        elif ev.buttons() == Qt.NoButton:
            self.hovered.emit(pos.x(), pos.y())

    def mouseReleaseEvent(self, ev):
        if ev.buttons() == Qt.LeftButton:
            pos = ev.pos()
            self.released.emit(pos.x(), pos.y())


class MediaCapture:

    def __init__(self, path):
        self.isImage = False
        try:
            self.media = cv2.VideoCapture(path)
            assert self.media.isOpened()
        except AssertionError:
            # sometimes image has bad format that cv2.VideoCapture
            # is unable to open, but cv2.imread can.
            self.media = cv2.imread(path)
            self.isImage = True

    def isOpened(self):
        if self.isImage and self.media.data is not None:
            return True
        return self.media.isOpened()

    def get(self, prop):
        if self.isImage:
            if prop == cv2.CAP_PROP_FRAME_COUNT:
                return 1
            elif prop == cv2.CAP_PROP_FPS:
                return 25.
            elif prop == cv2.CAP_PROP_FRAME_WIDTH:
                return self.media.shape[1]
            elif prop == cv2.CAP_PROP_FRAME_HEIGHT:
                return self.media.shape[0]
        return self.media.get(prop)

    def set(self, prop, value):
        if not self.isImage:
            return self.media.set(prop, value)

    def read(self, image=None):
        if self.isImage:
            return self.media.data is not None, self.media
        return self.media.read(image)

    def release(self):
        if self.isImage:
            self.media.data = None
        else:
            self.media.release()


class QMeadiaViewer(QOpenCVLabel):
    lengthChanged = pyqtSignal(int)
    positionChanged = pyqtSignal(int)
    sizeChanged = pyqtSignal(int, int)
    stateChanged = pyqtSignal(bool)
    ended = pyqtSignal()

    def __init__(self, parent=None):
        QOpenCVLabel.__init__(self, parent)
        self.setCursor(Qt.CrossCursor)
        self.length = 0
        self.duration = 0
        self.position = 0
        self.fps = 30
        self.media = None
        self.openCVFrame = None
        self.imageSize = None
        self.overlays = list()
        self.setOpenCVImage(np.ones((self.height(), self.width(), 3), dtype=np.uint8) * 200)
        self.lock = threading.Lock()
        self.thread = self._initCaptureThread()

    def _initCaptureThread(self):
        thread = QUpdater(interval=1/self.fps)
        thread.update.connect(self.increment)
        thread.start()
        return thread

    def setMedia(self, url):
        if len(url) and os.path.exists(url):
            self.thread.disconnect()
            if not self.thread.paused:
                self.thread.stop()
            if self.media is not None:
                self.media.release()
            self.media = MediaCapture(url)
            self.length = int(self.media.get(cv2.CAP_PROP_FRAME_COUNT))
            self.fps = float(self.media.get(cv2.CAP_PROP_FPS))
            self.imageSize = (int(self.media.get(cv2.CAP_PROP_FRAME_WIDTH)), int(self.media.get(cv2.CAP_PROP_FRAME_HEIGHT)))
            self.duration = self.length / self.fps if self.length > 1 else 0
            self.lengthChanged.emit(self.length)
            self.sizeChanged.emit(self.imageSize[0], self.imageSize[1])
            self.openCVFrame = None
            self.seek(0)
            self.thread = self._initCaptureThread()

    def redraw(self):
        if self.openCVFrame is not None:
            self.setOpenCVImage(self.overlay(self.openCVFrame))

    def seek(self, position):
        if self.length > 0:
            position = self._clamp(position)
            if self.openCVFrame is not None and self.position == position:
                return
            if not position == self.position + 1:
                self.media.set(cv2.CAP_PROP_POS_FRAMES, position)
            if position == self.length - 1:
                self.pause()
                self.ended.emit()
            self.position = position
            _, self.openCVFrame = self.media.read()
            self.setOpenCVImage(self.overlay(self.openCVFrame))
            self.positionChanged.emit(self.position)

    def increment(self, step=1):
        self.seek(self.position + step)

    def pause(self):
        self.thread.pause()
        self.stateChanged.emit(False)

    def unpause(self):
        self.thread.unpause()
        self.stateChanged.emit(True)

    def exit(self):
        self.thread.stop()
        self.thread.quit()

    @property
    def playing(self):
        return not self.thread.paused

    @property
    def elapsed(self):
        if self.length > 1:
            return self.position / (self.length - 1) * self.duration
        return 0

    def overlay(self, cvImage):
        for overlay in self.overlays:
            cvImage = overlay(cvImage)
        return cvImage

    def _clamp(self, position):
        if position < 0:
            position = 0
        if position > self.length - 1:
            position = self.length - 1
        return round(position)


class QPredictionBar(QLabel):
    clicked = pyqtSignal(QPoint)
    dragged = pyqtSignal(QPoint)
    lengthChanged = pyqtSignal(int)
    predictionsChanged = pyqtSignal()
    predictionsAdded = pyqtSignal(object)
    annotationsAdded = pyqtSignal(object)
    annotationsChanged = pyqtSignal()
    predictionsRemoved = pyqtSignal(object)
    annotationsRemoved = pyqtSignal(object)
    idsChanged = pyqtSignal(object)
    thresholdChanged = pyqtSignal(float)
    cmap = {  # i: np.array(v, dtype=np.uint8) for i, v in enumerate([
        None: [62, 80, 128],
        0: [180, 180, 180],
        1: [200, 200, 0],
        2: [255, 0, 0],
        3: [0, 255, 0],
        4: [200, 0, 200],
    }

    def __init__(self, parent=None):
        QLabel.__init__(self, parent)
        self.threshold = 0
        self.setMouseTracking(True)
        self.setMinimumSize(1, 1)
        self.length = 1
        self.annotations = SortedDict()
        self.predictions = SortedDict()
        self.pixels = {0: SortedSet(), 1: SortedSet(), 2: SortedSet()}
        self.cvImage = np.zeros((1, 1, 3), dtype=np.uint8)
        self.ids = SortedSet()
        self.filtered_ids = SortedSet()
        self.idsChanged.connect(self.filterIds)
        self.thresholdChanged.connect(self.filterIds)
        self.lengthChanged.connect(self.resetIds)
        self.annotationsAdded.connect(self.addIdsFromDict)
        self.predictionsAdded.connect(self.addIdsFromDict)
        self.predictionsAdded.connect(self.predictionsChanged)
        self.predictionsRemoved.connect(self.predictionsChanged)
        self.annotationsAdded.connect(self.annotationsChanged)
        self.annotationsRemoved.connect(self.annotationsChanged)
        self.show()

    def filterIds(self):
        self.filtered_ids = SortedSet(filter(lambda id: id == 0 or
                                                        id == self.length - 1 or
                                                        (id in self.predictions and self.predictions[id][0][-1] >= self.threshold) or
                                                        (id in self.annotations),
                                             self.ids))

    def setThreshold(self, threshold):
        self.threshold = threshold
        self.thresholdChanged.emit(self.threshold)

    def resetIds(self):
        self.ids.clear()
        self.ids.add(0)
        if self.length > 0:
            self.ids.add(self.length - 1)
        self.idsChanged.emit(self.ids)

    def addIdsFromDict(self, additions=None):
        if additions is not None:
            for _ in additions.keys():
                self.ids.add(_)
        self.idsChanged.emit(self.ids)

    def addId(self, id):
        if id not in self.ids:
            self.ids.add(id)
            self.idsChanged.emit(self.ids)

    def removeId(self, id):
        if id not in set(self.annotations.keys()).union(self.predictions.keys()):
            self.ids.remove(id)
            self.idsChanged.emit(self.ids)

    def setLength(self, length):
        self.length = length
        self.clear()
        self.lengthChanged.emit(self.length)

    def setPredictions(self, predictions):
        self.clearPredictions(False)
        self.predictions.update(predictions)
        self.predictionsAdded.emit(predictions)
        self.redraw()

    def addPredictions(self, predictions):
        self.predictions.update(predictions)
        self.predictionsAdded.emit(predictions)
        self.redraw()

    def removePrediction(self, id):
        if id in self.predictions:
            for _ in [2]:
                self.pixels[_].pop(id)
            self.predictionsRemoved.emit({id: self.predictions.pop(id)})
            self.redraw()

    def setAnnotations(self, annotations):
        self.clearAnnotations(False)
        self.annotations.update(annotations)
        self.annotationsAdded.emit(annotations)
        self.redraw()

    def addAnnotations(self, annotations):
        self.annotations.update(annotations)
        self.annotationsAdded.emit(annotations)
        self.redraw()

    def removeAnnotation(self, id):
        if id in self.annotations:
            for _ in [0, 1]:
                self.pixels[_].clear()
            self.annotationsRemoved.emit({id: self.annotations.pop(id)})
            self.redraw()

    def redraw(self):
        scale = (self.width() - 1) / (self.length - 1) if self.length > 1 else 0
        for _ in self.pixels.keys():
            self.pixels[_].clear()
        self.pixels[0].update(map(lambda ann: round(ann[0] * scale), filter(lambda ann: len(ann[1]) == 0, self.annotations.items())))
        self.pixels[1].update(map(lambda ann: round(ann[0] * scale), filter(lambda ann: len(ann[1]) > 0, self.annotations.items())))
        self.pixels[2].update(map(lambda pred: round(pred[0] * scale),
                                  filter(lambda pred: any(obj[-1] >= self.threshold for obj in pred[1]),
                                         self.predictions.items())))
        self.show()

    def show(self):
        if self.cvImage.shape[1] != self.width():
            self.cvImage = np.zeros((1, self.width(), 3), dtype=np.uint8)
        self.cvImage[:, :] = self.cmap[None]
        for _ in [0, 1]:
            if len(self.pixels[_]) > 0:
                self.cvImage[:, list(self.pixels[_])] = self.cmap[_]
        if len(self.pixels[2]) > 0:
            pred = set(self.pixels[2])
            fn = tuple(pred.difference(list(self.pixels[0]) + list(self.pixels[1])))
            fp = tuple(pred.intersection(self.pixels[0]))
            tp = tuple(pred.intersection(self.pixels[1]))
            self.cvImage[:, fn] = self.cmap[2]
            self.cvImage[:, tp] = self.cmap[3]
            self.cvImage[:, fp] = self.cmap[4]
        pixmap = QPixmap.fromImage(QImage(self.cvImage.data, self.cvImage.shape[1], self.cvImage.shape[0], QImage.Format_RGB888))
        self.setPixmap(pixmap.scaled(self.size()))

    def clearPredictions(self, redraw=True):
        self.predictions.clear()
        for _ in [0, 1]:
            self.pixels[_].clear()
        if redraw:
            self.redraw()

    def clearAnnotations(self, redraw=True):
        self.annotations.clear()
        for _ in [2]:
            self.pixels[_].clear()
        if redraw:
            self.redraw()

    def clear(self, redraw=True):
        self.clearPredictions(False)
        self.clearAnnotations(False)
        self.resetIds()
        if redraw:
            self.redraw()

    def resizeEvent(self, ev):
        self.cvImage = np.zeros((1, self.width(), 3), dtype=np.uint8)
        for _ in self.pixels.keys():
            self.pixels[_].clear()
        self.redraw()

    def mousePressEvent(self, ev):
        if ev.button() == Qt.LeftButton:
            self.clicked.emit(ev.pos())

    def mouseMoveEvent(self, ev):
        if ev.buttons() == Qt.LeftButton:
            self.dragged.emit(ev.pos())


class QClickableSlider(QSlider):
    clicked = pyqtSignal(int)
    dragged = pyqtSignal(int)

    def mousePressEvent(self, ev):
        if ev.button() == QtCore.Qt.LeftButton:
            self.clicked.emit(self._pixelPosToRangeValue(ev.pos()))

    def mouseMoveEvent(self, ev):
        if ev.buttons() == Qt.LeftButton:
            self.dragged.emit(self._pixelPosToRangeValue(ev.pos()))

    def _pixelPosToRangeValue(self, pos):
        opt = QtWidgets.QStyleOptionSlider()
        self.initStyleOption(opt)
        gr = self.style().subControlRect(QtWidgets.QStyle.CC_Slider, opt, QtWidgets.QStyle.SC_SliderGroove, self)
        sr = self.style().subControlRect(QtWidgets.QStyle.CC_Slider, opt, QtWidgets.QStyle.SC_SliderHandle, self)

        if self.orientation() == QtCore.Qt.Horizontal:
            sliderLength = sr.width()
            sliderMin = gr.x()
            sliderMax = gr.right() - sliderLength + 1
        else:
            sliderLength = sr.height()
            sliderMin = gr.y()
            sliderMax = gr.bottom() - sliderLength + 1
        pr = pos - sr.center() + sr.topLeft()
        p = pr.x() if self.orientation() == QtCore.Qt.Horizontal else pr.y()
        return QtWidgets.QStyle.sliderValueFromPosition(self.minimum(), self.maximum(), p - sliderMin,
                                                        sliderMax - sliderMin, opt.upsideDown)


class QBBoxView(QListView):
    removed = pyqtSignal(int)
    cleared = pyqtSignal()
    selected = pyqtSignal(int)

    def __init__(self, enable_context=True):
        super().__init__()
        self.entry = QStandardItemModel()
        self.setModel(self.entry)
        self.setEditTriggers(QAbstractItemView.NoEditTriggers)
        self.setSelectionMode(QAbstractItemView.NoSelection)
        self.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.setFocusPolicy(Qt.NoFocus)
        self.setMouseTracking(True)
        self.installEventFilter(self)
        self.selectedId = -1
        self.enable_context = enable_context

    def copyFrom(self, items):
        self.entry.clear()
        if len(items) == 0:
            self.entry.appendRow(QStandardItem("<negative>"))
        else:
            for item in items:
                self.add(item)

    def add(self, item):
        rep = f"flag - {[int(_) for _ in item[:4]]}"
        if len(item) == 5:
            rep += f" - {item[-1]:.3%}"
        self.entry.appendRow(QStandardItem(rep))

    def addAll(self, items):
        for item in items:
            self.add(item)

    def requestRemove(self, id):
        self.removed.emit(id)

    def requestClear(self):
        self.cleared.emit()

    def select(self, id):
        self.selectedId = id
        self.selected.emit(id)

    def eventFilter(self, source, event):
        if source is self and hasattr(event, 'pos'):
            index = self.indexAt(event.pos()).row()
            if event.type() == QtCore.QEvent.ContextMenu and self.enable_context:
                menu = QMenu("Menu", self)
                if index >= 0:
                    deleteAction = QAction(QIcon(), '&Delete', self)
                    deleteAction.setStatusTip('Delete')
                    deleteAction.triggered.connect(lambda *args: self.requestRemove(index))

                    menu.addAction(deleteAction)
                else:
                    clearAction = QAction(QIcon(), '&Clear', self)
                    clearAction.setStatusTip('Clear')
                    clearAction.triggered.connect(self.requestClear)

                    menu.addAction(clearAction)
                menu.exec_(event.globalPos())
                return True
            self.select(index)
        return super().eventFilter(source, event)


class QPreferenceDialog(QDialog):

    def __init__(self, app, parent=None, size=QSize(200, 120)):
        QDialog.__init__(self, parent)
        self.setWindowTitle("Preferences")
        self.app = app

        self.note = QLabel("Tool by MICA")
        self.buttonBox = QDialogButtonBox(self)
        self.buttonBox.setStandardButtons(QDialogButtonBox.Cancel | QDialogButtonBox.Ok)
        self.buttonBox.setOrientation(Qt.Horizontal)
        self.buttonBox.accepted.connect(self.accept)
        self.buttonBox.rejected.connect(self.reject)

        layout = QVBoxLayout(self)
        layout.addWidget(self.note)
        layout.addWidget(self.buttonBox)
        layout.setAlignment(self.note, Qt.AlignCenter)
        layout.setAlignment(self.buttonBox, Qt.AlignCenter)

        stylesheet = '''
        QSlider::groove:horizontal {
            border: 1px solid;
            height: 10px;
            margin: 0px;
            }
        QSlider::sub-page:horizontal {
            border: 1px solid #777;
            height: 10px;
            border-radius: 4px;
            }
        QSlider::handle:horizontal {
            border: 1px solid;
            height: 30px;
            width: 1px;
            margin: -2px 0px;
            }
        '''
        try:
            stylesheet = __import__('qdarkstyle').load_stylesheet_pyqt5() + stylesheet
        except:
            pass
        finally:
            self.setStyleSheet(stylesheet)
        self.setFocus()
        self.resize(size)
        self.setMaximumSize(size)
        self.setMinimumSize(size)


class HDMainWindow(QMainWindow):
    lengthChanged = pyqtSignal(int)

    def __init__(self, app, parent=None):
        QMainWindow.__init__(self, parent)
        self.home = os.path.dirname(__file__)
        self.setWindowTitle("Flag Detection Demo Tool")
        self.setWindowIcon(self.style().standardIcon(QStyle.SP_VistaShield))
        self.app = app

        self.mediaFilePosition = 0
        self.mediaList = []
        self.mediaFile = None
        self.mediaViewer = QMeadiaViewer()
        self.mediaViewer.setMinimumSize(520, 360)
        self.mediaViewer.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.MinimumExpanding)
        self.mediaViewer.setAlignment(Qt.AlignCenter)

        self.icons = OrderedDict(
            play=self.style().standardIcon(QStyle.SP_MediaPlay),
            pause=self.style().standardIcon(QStyle.SP_MediaPause),
            seekforward=self.style().standardIcon(QStyle.SP_MediaSeekForward),
            seekbackward=self.style().standardIcon(QStyle.SP_MediaSeekBackward),
            skipforward=self.style().standardIcon(QStyle.SP_MediaSkipForward),
            skipbackward=self.style().standardIcon(QStyle.SP_MediaSkipBackward),
            prevfile=self.style().standardIcon(QStyle.SP_ArrowBack),
            nextfile=self.style().standardIcon(QStyle.SP_ArrowRight),
        )
        self.buttons = OrderedDict(
            prevfile=QPushButton(),
            skipbackward=QPushButton(),
            seekbackward=QPushButton(),
            play=QPushButton(),
            seekforward=QPushButton(),
            skipforward=QPushButton(),
            nextfile=QPushButton(),
        )
        for name, btn in self.buttons.items():
            self.buttons[name].setIcon(self.icons[name])
            btn.setCursor(Qt.PointingHandCursor)
            btn.setEnabled(False)
            btn.setSizePolicy(QSizePolicy.Minimum, QSizePolicy.Fixed)
            btn.setMaximumWidth(40)
            btn.setMinimumWidth(40)
        self.mediaViewer.lengthChanged.connect(
            lambda length: [btn.setEnabled(length > 1) for btn in list(self.buttons.values())[1:-1]])
        self.buttons['play'].clicked.connect(self.play)
        self.mediaViewer.stateChanged.connect(
            lambda playing: self.buttons['play'].setIcon(self.icons['play' if not playing else 'pause']))
        # self.mediaViewer.clicked.connect(lambda *args: self.buttons['play'].animateClick())
        self.buttons['seekbackward'].clicked.connect(lambda *args: self.seekStep(-10))
        self.buttons['seekforward'].clicked.connect(lambda *args: self.seekStep(10))
        self.buttons['skipbackward'].clicked.connect(lambda *args: self.seekPrediction(-1))
        self.buttons['skipforward'].clicked.connect(lambda *args: self.seekPrediction(1))
        self.buttons['prevfile'].clicked.connect(lambda *args: self.seekFileStep(-1))
        self.buttons['nextfile'].clicked.connect(lambda *args: self.seekFileStep(1))

        self.predictionBar = QPredictionBar()
        self.predictionBar.setMinimumHeight(20)
        self.predictionBar.setMaximumHeight(20)
        self.predictionBar.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed)
        self.predictionBar.clicked.connect(lambda pos: self.mediaViewer.seek(pos.x() / self.predictionBar.width() * self.mediaViewer.length))
        self.predictionBar.dragged.connect(lambda pos: self.mediaViewer.seek(pos.x() / self.predictionBar.width() * self.mediaViewer.length))
        self.mediaViewer.lengthChanged.connect(self.predictionBar.setLength)
        self.mediaViewer.overlays.append(self.visualizePrediction)
        self.mediaViewer.overlays.append(self.visualizeAnnotation)
        self.mediaViewer.clicked.connect(self.addAnnotation)
        self.mediaViewer.dragged.connect(self.editAnnotation)
        self.mediaViewer.released.connect(self.finalizeAnnotation)

        self.positionSlider = QClickableSlider(Qt.Horizontal)
        self.positionSlider.setRange(0, 0)
        self.positionSlider.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.mediaViewer.lengthChanged.connect(lambda length: self.positionSlider.setRange(0, length - 1))
        self.mediaViewer.positionChanged.connect(self.positionSlider.setValue)
        self.positionSlider.clicked.connect(self.mediaViewer.seek)
        self.positionSlider.dragged.connect(self.mediaViewer.seek)
        self.positionSlider.sliderMoved.connect(self.mediaViewer.seek)
        self.positionSlider.sliderPressed.connect(lambda *args: self.mediaViewer.thread.block())
        self.positionSlider.sliderReleased.connect(lambda *args: self.mediaViewer.thread.unblock())

        self.positionLabel = QEventLabel("[please open a media file]")
        self.positionLabel.setFont(QFont("Consolas", 9, QFont.Bold))
        self.positionLabel.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.positionLabel.setAlignment(Qt.AlignCenter)
        self.positionLabel.mouseEntered.connect(self.showPosition)
        self.positionLabel.mouseLeft.connect(self.showPosition)
        self.mediaViewer.lengthChanged.connect(self.showPosition)
        self.mediaViewer.positionChanged.connect(self.showPosition)
        self.infoLabel = QEventLabel("MICA for-fun demo application, no warranty, "
                                     "lots of bugs and unimplemented features!")
        self.infoLabel.setFont(QFont("Consolas", 9, QFont.Bold))
        self.infoLabel.setSizePolicy(QSizePolicy.Preferred, QSizePolicy.Maximum)
        self.infoLabel.setCursor(Qt.WhatsThisCursor)

        self.annotationView = QBBoxView()
        self.annotationView.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.MinimumExpanding)
        self.annotationView.setMaximumWidth(220)
        self.mediaViewer.positionChanged.connect(self.updateAnnotationView)
        self.mediaViewer.lengthChanged.connect(self.updateAnnotationView)
        self.predictionBar.annotationsChanged.connect(self.updateAnnotationView)
        self.predictionBar.annotationsChanged.connect(self.mediaViewer.redraw)
        self.annotationView.selected.connect(self.mediaViewer.redraw)
        self.annotationView.removed.connect(self.removeAnnotation)
        self.annotationView.cleared.connect(self.clearAnnotation)

        self.predictionView = QBBoxView(enable_context=False)
        self.predictionView.setSizePolicy(QSizePolicy.Fixed, QSizePolicy.MinimumExpanding)
        self.predictionView.setMaximumWidth(220)
        self.mediaViewer.positionChanged.connect(self.updatePredictionView)
        self.mediaViewer.lengthChanged.connect(self.updatePredictionView)
        self.predictionBar.predictionsChanged.connect(self.updatePredictionView)
        self.predictionBar.predictionsChanged.connect(self.mediaViewer.redraw)
        self.predictionView.selected.connect(self.mediaViewer.redraw)

        self.confidenceSlider = QSlider(Qt.Horizontal)
        self.confidenceSlider.setRange(0, 400)
        self.confidenceSlider.setValue(int(self.confidenceSlider.maximum() * .25))
        self.confidenceSlider.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Fixed)
        self.confidenceSlider.setFixedHeight(8)
        # self.confidenceSlider.clicked.connect(self.mediaViewer.redraw)
        # self.confidenceSlider.dragged.connect(self.mediaViewer.redraw)
        self.confidenceSlider.sliderMoved.connect(self.mediaViewer.redraw)
        self.confidenceLabel = QLabel(
            f"{self.confidenceSlider.value() / self.confidenceSlider.maximum():.02%}".rjust(6, "0"))
        self.confidenceSlider.valueChanged.connect(
            lambda val: self.confidenceLabel.setText(f"{val / self.confidenceSlider.maximum():.02%}".rjust(6, "0")))
        self.confidenceSlider.valueChanged.connect(
            lambda val: self.predictionBar.setThreshold(val / self.confidenceSlider.maximum()))
        self.confidenceSlider.valueChanged.connect(self.predictionBar.redraw)
        self.predictionBar.setThreshold(self.confidenceSlider.value() / self.confidenceSlider.maximum())

        # Create open actions
        openMediaAction = QAction(QIcon(), '&Open Media', self)
        openMediaAction.setShortcut('Ctrl+O')
        openMediaAction.setStatusTip('Open video or image file')
        openMediaAction.triggered.connect(self.openMediaFile)

        openPredictionAction = QAction(QIcon(), '&Open Prediction', self)
        openPredictionAction.setShortcut('Ctrl+P')
        openPredictionAction.setStatusTip('Open prediction file')
        openPredictionAction.triggered.connect(self.openPredictionFile)

        openDirectoryAction = QAction(QIcon(), '&Open Directory', self)
        openDirectoryAction.setShortcut('Ctrl+L')
        openDirectoryAction.setStatusTip('Open directory')
        openDirectoryAction.triggered.connect(self.openDirectory)

        seekFileAction = QAction(QIcon(), '&Goto File', self)
        seekFileAction.setShortcut('Ctrl+G')
        seekFileAction.setStatusTip('Goto file in list')
        seekFileAction.triggered.connect(self.chooseFile)

        saveAnnotationAction = QAction(QIcon(), '&Save', self)
        saveAnnotationAction.setShortcut('Ctrl+S')
        saveAnnotationAction.setStatusTip('Save annotation')
        saveAnnotationAction.triggered.connect(self.saveAnnotation)

        # Exit action
        exitAction = QAction(QIcon(), '&Exit', self)
        exitAction.setShortcut('Ctrl+Q')
        exitAction.setStatusTip('Exit application')
        exitAction.triggered.connect(self.exit)

        # Setting action
        settingAction = QAction(QIcon(), '&Preferences', self)
        settingAction.setShortcut('Alt+P')
        settingAction.setStatusTip('Change preferences')
        settingAction.triggered.connect(self.openSettings)

        # Menu bar
        menuBar = self.menuBar()
        fileMenu = menuBar.addMenu('&File')
        fileMenu.addAction(openMediaAction)
        fileMenu.addAction(openPredictionAction)
        fileMenu.addAction(openDirectoryAction)
        fileMenu.addSeparator()
        fileMenu.addAction(seekFileAction)
        fileMenu.addSeparator()
        fileMenu.addAction(saveAnnotationAction)
        fileMenu.addSeparator()
        fileMenu.addAction(exitAction)
        windowMenu = menuBar.addMenu('&Window')
        windowMenu.addAction(settingAction)

        # Create layouts
        btnLayout = QHBoxLayout()
        btnLayout.setContentsMargins(0, 0, 0, 0)
        for btn in self.buttons.values():
            btnLayout.addWidget(btn)

        controlLayout = QVBoxLayout()
        controlLayout.setContentsMargins(0, 0, 0, 0)
        controlLayout.setSpacing(3)
        controlLayout.addWidget(self.positionLabel)
        controlLayout.addLayout(btnLayout)

        sliderLayout = QVBoxLayout()
        sliderLayout.setContentsMargins(0, 0, 0, 0)
        sliderLayout.setSpacing(3)
        sliderLayout.addWidget(self.predictionBar)
        sliderLayout.addWidget(self.positionSlider)

        bottomLayout = QHBoxLayout()
        bottomLayout.setContentsMargins(0, 0, 0, 0)
        bottomLayout.addLayout(controlLayout)
        bottomLayout.addLayout(sliderLayout)

        # Tuan
        actions, numRow, numCol = self.loadActions()
        self.tableView = QTableViewer(actions, numRow, numCol)
        self.tableView.selectionModel().selectionChanged.connect(self.selectTableRow)

        annotationLayout = QVBoxLayout()
        annotationLayout.setContentsMargins(0, 0, 0, 3)
        annotationLayout.addWidget(QLabel("Annotations"))
        #annotationLayout.addWidget(self.annotationView)
        annotationLayout.addWidget(self.tableView)
        annotationWidget = QWidget()
        annotationWidget.setLayout(annotationLayout)

        predictionLayout = QVBoxLayout()
        predictionLayout.setContentsMargins(0, 3, 0, 0)
        predictionLayout.addWidget(QLabel("Predictions"))
        predictionLayout.addWidget(self.predictionView)
        predictionWidget = QWidget()
        predictionWidget.setLayout(predictionLayout)

        confidenceSliderLayout = QHBoxLayout()
        confidenceSliderLayout.addWidget(self.confidenceSlider)
        confidenceSliderLayout.addWidget(self.confidenceLabel)
        confidenceLayout = QVBoxLayout()
        confidenceLayout.setContentsMargins(0, 3, 0, 0)
        confidenceLayout.addWidget(QLabel("Confidence Threshold"))
        confidenceLayout.addLayout(confidenceSliderLayout)
        confidenceWidget = QWidget()
        confidenceWidget.setLayout(confidenceLayout)

        annotationSplitter = QSplitter(Qt.Vertical)
        annotationSplitter.addWidget(annotationWidget)
        annotationSplitter.addWidget(predictionWidget)
        annotationSplitter.addWidget(confidenceWidget)

        viewerLayout = QHBoxLayout()
        viewerLayout.setContentsMargins(0, 0, 0, 0)
        viewerLayout.setSpacing(3)
        viewerLayout.addWidget(annotationSplitter)
        viewerLayout.addWidget(self.mediaViewer)

        layout = QVBoxLayout()
        layout.addLayout(viewerLayout)
        layout.addLayout(bottomLayout)
        layout.addWidget(self.infoLabel)

        stylesheet = '''
        QSlider::groove:horizontal {
            border: 1px solid;
            height: 10px;
            margin: 0px;
            }
        QSlider::sub-page:horizontal {
            border: 1px solid #777;
            height: 10px;
            border-radius: 4px;
            }
        QSlider::handle:horizontal {
            border: 1px solid;
            height: 30px;
            width: 1px;
            margin: -2px 0px;
            }
        '''
        try:
            stylesheet = __import__('qdarkstyle').load_stylesheet_pyqt5() + stylesheet
        except (ModuleNotFoundError, ImportError):
            pass
        finally:
            self.setStyleSheet(stylesheet)
        self.setChildrenFocusPolicy(Qt.NoFocus)
        self.app.installEventFilter(self)
        self.setFocus()

        # Create a widget for window contents
        wid = QWidget(self)
        wid.setLayout(layout)
        self.setCentralWidget(wid)


    def selectTableRow(self, selected, deselected):
    	# Lay thoi gian cua dong dang chon => convert thanh so giay => convert sang gia tri position (so giay * fps (30))
    	# Lay start cua row dang chon
    	currentRow = self.tableView.currentRow()
    	start = self.tableView.item(currentRow, 0).text()
    	# Convert string start sang so giay
    	tmp = datetime.datetime.strptime(start, '%H:%M:%S.%f')
    	seconds = datetime.timedelta(hours=tmp.hour,
                                     minutes=tmp.minute,
                                     seconds=tmp.second,
                                     milliseconds=tmp.microsecond/1000).total_seconds()
    	position = int(seconds * self.mediaViewer.fps)
    	print(seconds, position)
    	self.mediaViewer.seek(position)

    def loadActions(self):
    	# TODO Dynamic file path
    	df = pd.read_csv('table.csv') # đọc dữ liệu từ file CSV và lưu vào dataframe 
    	actions = {} # kiểu từ điển 
    	for (columnName, columnData) in df.iteritems():
    		actions[columnName] = columnData.values.astype('str').tolist()
    	return actions, df.shape[0], df.shape[1]

    def openMediaFile(self, fileName=''):
        if isinstance(fileName, bool):
            fileName, _ = QFileDialog.getOpenFileName(self, "Open Media", self.home)
        if len(fileName) == 0:
            return
        elif os.path.exists(fileName):
            self.mediaViewer.setMedia(fileName)
            self.predictionBar.clear()
            self.setWindowTitle(os.path.basename(fileName))
            self.mediaFile = fileName
        else:
            self.showDialog(title='Error', text=f'File not fould: {fileName}.')

    def openPredictionFile(self, fileName=''):
        if self.mediaViewer.length == 0:
            self.showDialog(title='Error', text='Please open media file first.')
            return
        if isinstance(fileName, bool):
            fileName, _ = QFileDialog.getOpenFileName(self, "Open Prediction", self.home)
        if len(fileName) == 0:
            return
        elif os.path.exists(fileName):
            with open(fileName, 'r') as f:
                lines = [line.rstrip('\n') for line in f]
            predictions = SortedDict({int(line[0]): [line[i: i+5] for i in range(2, len(line), 5)]
                                      for line in [list(map(lambda _: float(_), line.split())) for line in lines[1:]]
                                      if int(line[0]) < self.mediaViewer.length})
            self.predictionBar.clear()
            self.predictionBar.setPredictions(predictions)
        else:
            self.showDialog(title='Error', text=f'File not fould: {fileName}.')

    def openDirectory(self, dirName=''):
        self.showDialog(title='Error', text=f'Features not implemented!')

    def chooseFile(self, position=0):
        okPressed = True
        if isinstance(position, bool):
            position, okPressed = QInputDialog.getInt(self, "Goto File", "File rank: (0-{})".format(len(self.mediaList) - 1),
                                                      0, 0, len(self.mediaList) - 1, 1)
        if okPressed:
            self.seekFile(position)

    def seekFile(self, position):
        if len(self.mediaList) > 1:
            self.mediaFilePosition = round(position)
            if self.mediaFilePosition < 0:
                self.mediaFilePosition = 0
            elif self.mediaFilePosition >= len(self.mediaList):
                self.mediaFilePosition = len(self.mediaList) - 1
            pass

    def seekFileStep(self, step):
        if len(self.mediaList) > 1 and step != 0:
            position = self.mediaFilePosition + step
            self.seekFile(position)

    def saveAnnotation(self):
        self.showDialog(title='Error', text='Features not implemented!')

    def showPosition(self):
        if self.mediaViewer.length > 0:
            position, duration = (self.mediaViewer.length - 1, self.mediaViewer.duration) if self.positionLabel.mouseOver else (self.mediaViewer.position, self.mediaViewer.elapsed)
            text = f"[{position}] {self.format_duration(duration)}"
            # if len(self.mediaList) > 0:
            #     text = f"[{self.mediaFilePosition}/{len(self.mediaList)}]-" + text
            self.positionLabel.setText(text)

    def play(self):
        if not self.mediaViewer.playing:
            self.mediaViewer.unpause()
        else:
            self.mediaViewer.pause()

    def seekStep(self, step=1):
        self.mediaViewer.increment(step)

    def seekPrediction(self, direction=1):
        if self.mediaViewer.length > 0:
            if direction == 0:
                return
            ids = self.predictionBar.filtered_ids
            if direction > 0:
                fid = max(0, min(bisect.bisect_right(ids, self.mediaViewer.position), len(ids) - 1))
            else:
                fid = max(0, min(bisect.bisect_left(ids, self.mediaViewer.position) - 1, len(ids) - 1))
            self.mediaViewer.seek(ids[fid])

    def visualizePrediction(self, cvImage):
        if self.predictionBar.length > 0 and self.mediaViewer.position in self.predictionBar.predictions:
            cvImage = cvImage.copy()
            bboxes = self.predictionBar.predictions[self.mediaViewer.position]
            confidence_threshold = self.confidenceSlider.value() / self.confidenceSlider.maximum()
            for id, bbox in enumerate(bboxes):
                # bbox_start_index = 1 if len(bbox) > 4 else 0
                x1, y1, x2, y2 = (round(_) for _ in bbox[:4])
                # object_id = bbox[0] if len(bbox) > 4 else 'object'
                confidence = bbox[-1] if len(bbox) > 4 else None
                if confidence is not None and confidence < confidence_threshold:
                    continue
                cv2.rectangle(cvImage,
                              (x1, y1), (x2, y2),
                              [0, 0, 255] if id == self.predictionView.selectedId else [0, 0, 200], 2)
                cv2.putText(cvImage, f'{confidence:.03%}',
                            (int(x1), int(y1) - 4),
                            cv2.FONT_HERSHEY_SIMPLEX, 0.5,
                            [0, 0, 255] if id == self.predictionView.selectedId else [0, 0, 200], 2)
        return cvImage

    def visualizeAnnotation(self, cvImage):
        if self.predictionBar.length > 0 and self.mediaViewer.position in self.predictionBar.annotations:
            cvImage = cvImage.copy()
            bboxes = self.predictionBar.annotations[self.mediaViewer.position]
            for id, bbox in enumerate(bboxes):
                x1, y1, x2, y2 = [round(_) for _ in bbox[:4]]
                cv2.rectangle(cvImage,
                              (x1, y1), (x2, y2),
                              [0, 255, 255] if id == self.annotationView.selectedId else [0, 200, 200], 2)
        return cvImage

    def updateAnnotationView(self):
        if self.mediaViewer.position in self.predictionBar.annotations:
            self.annotationView.copyFrom(self.predictionBar.annotations[self.mediaViewer.position])
        else:
            self.annotationView.entry.clear()

    def updatePredictionView(self):
        if self.mediaViewer.position in self.predictionBar.predictions:
            self.predictionView.copyFrom(self.predictionBar.predictions[self.mediaViewer.position])
        else:
            self.predictionView.entry.clear()

    def addAnnotation(self, x, y):
        if self.mediaViewer.length > 0 and not self.mediaViewer.playing:
            viewerSize = self.mediaViewer.size()
            x = x / viewerSize.width() * self.fmediaViewer.imageSize[0]
            y = y / viewerSize.height() * self.mediaViewer.imageSize[1]
            if not self.mediaViewer.position in self.predictionBar.annotations:
                self.predictionBar.addAnnotations({self.mediaViewer.position: [[x, y, x, y]]})
            else:
                self.predictionBar.annotations[self.mediaViewer.position].append([x, y, x, y])
                self.predictionBar.annotationsChanged.emit()

    def editAnnotation(self, x, y):
        if self.mediaViewer.length > 0 and not self.mediaViewer.playing:
            viewerSize = self.mediaViewer.size()
            x = x / viewerSize.width() * self.mediaViewer.imageSize[0]
            y = y / viewerSize.height() * self.mediaViewer.imageSize[1]
            editting = self.predictionBar.annotations[self.mediaViewer.position][-1]
            editting[2:] = (x, y)
            self.predictionBar.annotationsChanged.emit()
            self.predictionBar.redraw()
            self.mediaViewer.redraw()

    def finalizeAnnotation(self):
        if self.mediaViewer.length > 0 and not self.mediaViewer.playing:
            editting = self.predictionBar.annotations[self.mediaViewer.position][-1]
            if int(editting[0]) == int(editting[2]) and int(editting[1]) == int(editting[3]):
                self.predictionBar.annotations[self.mediaViewer.position].pop()
                if len(self.predictionBar.annotations[self.mediaViewer.position]) == 0:
                    self.predictionBar.removeAnnotation(self.mediaViewer.position)
                self.mediaViewer.redraw()
            else:
                editting[0] = np.clip(editting[0], 0, self.mediaViewer.imageSize[0]).item()
                editting[2] = np.clip(editting[2], 0, self.mediaViewer.imageSize[0]).item()
                editting[1] = np.clip(editting[1], 0, self.mediaViewer.imageSize[1]).item()
                editting[3] = np.clip(editting[3], 0, self.mediaViewer.imageSize[1]).item()
                tmp = sorted((editting[:2], editting[2:]))
                editting = tmp[0] + tmp[1]
            self.predictionBar.annotationsChanged.emit()

    def removeAnnotation(self, id):
        if self.mediaViewer.length > 0 and self.mediaViewer.position in self.predictionBar.annotations and id < len(self.predictionBar.annotations[self.mediaViewer.position]):
            self.predictionBar.annotations[self.mediaViewer.position].pop(id)
            self.predictionBar.annotationsChanged.emit()
            if len(self.predictionBar.annotations[self.mediaViewer.position]) == 0:
                self.predictionBar.removeId(self.mediaViewer.position)

    def clearAnnotation(self):
        if self.mediaViewer.length > 0:
            self.predictionBar.removeAnnotation(self.mediaViewer.position)
            self.predictionBar.annotationsChanged.emit()
            self.predictionBar.removeId(self.mediaViewer.position)

    def openSettings(self, *args):
        preferenceDialog = QPreferenceDialog(self.app, self)
        if preferenceDialog.exec():
            pass

    def showDialog(self, title='Info', text=None, informative_text=None):
        msg = QMessageBox()
        msg.setIcon(QMessageBox.Critical)
        msg.setText(text)
        msg.setInformativeText(informative_text)
        msg.setWindowTitle(title)
        msg.exec_()

    def setChildrenFocusPolicy(self, policy):
        def recursiveSetChildFocusPolicy(parentQWidget):
            for childQWidget in parentQWidget.findChildren(QWidget):
                childQWidget.setFocusPolicy(policy)
                childQWidget.clearFocus()
                recursiveSetChildFocusPolicy(childQWidget)
        recursiveSetChildFocusPolicy(self)

    def eventFilter(self, source, event):
        if event.type() == QEvent.KeyPress:
            pressed_key = event.key()
            modifiers = QtWidgets.QApplication.keyboardModifiers()
            if pressed_key == Qt.Key_Space:
                self.buttons['play'].animateClick(1)
            elif pressed_key == Qt.Key_Left:
                if modifiers == Qt.ShiftModifier:
                    self.buttons['skipbackward'].animateClick(1)
                elif modifiers == Qt.ControlModifier:
                    self.buttons['prevfile'].animateClick(1)
                else:
                    self.buttons['seekbackward'].animateClick(1)
            elif pressed_key == Qt.Key_Right:
                if modifiers == Qt.ShiftModifier:
                    self.buttons['skipforward'].animateClick(1)
                elif modifiers == Qt.ControlModifier:
                    self.buttons['nextfile'].animateClick(1)
                else:
                    self.buttons['seekforward'].animateClick(1)
        return QMainWindow.eventFilter(self, source, event)

    def exit(self, returnCode=0):
        print('[Terminated]', end='\n\n')
        self.mediaViewer.exit()
        sys.exit(returnCode)

    @staticmethod
    def format_duration(duration):
        m, s = divmod(duration, 60)
        h, m = divmod(m, 60)
        ms = s % 1 * 1000
        if h > 0:
            return f"{int(h):02d}:{int(m):02d}:{int(s):02d}:{int(ms):03d}"
        return f"{int(m):02d}:{int(s):02d}:{int(ms):03d}"


def parse_args():
    parser = argparse.ArgumentParser(description="Chương trình demo bởi MICA.",
                                     add_help=False)
    parser.add_argument("--window_sizes", default="1080,720",
                        help="[tuple[int]] kích cỡ cửa sổ chương trình (ngăn cách bởi dấu phẩy ',').")
    parser.add_argument("--media", help="[str] Đường dẫn đến file ảnh/video.")
    parser.add_argument("--prediction", help="[str] Đường dẫn đến file kết quả nhận dạng.")

    parser.add_argument("-h", "--help", action="help", default=argparse.SUPPRESS,
                        help="[action] Hiển thị thông báo trợ giúp này.")
    args = parser.parse_args()

    args.window_sizes = tuple(map(lambda s: int(s), args.window_sizes.split(',')))
    assert len(args.window_sizes) == 2, "window_sizes must be tuple of 2 elements, got {}.".format(args.window_sizes)
    return args


def main():
    args = parse_args()

    app = QApplication(sys.argv)
    player = HDMainWindow(app)
    player.resize(*args.window_sizes)

    if args.media is not None:
        player.openMediaFile(args.media)
    if args.prediction is not None:
        player.openPredictionFile(args.prediction)

    player.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()