from smartcam.abstract import FrameWriter
import threading
import queue
import logging
from PIL import Image
import pickle

logger = logging.getLogger(__name__)

class FramePickler(FrameWriter):
  """ write images using PIL """

  def __init__(self, queue, cloud_writer, frame_converter=None):
    threading.Thread.__init__(self)
    self.queue = queue
    self.cloud_writer = cloud_writer
    self.frame_converter = frame_converter

  def serialize(self, frame):
    return pickle.dumps(frame)

  def write_frame(self, frame):
    if self.frame_converter:
      frame.image = Image.fromarray(self.frame_converter(img))
    self.cloud_writer.write_fileobj(self.serialize(frame))

  def run(self):
    logger.debug("starting FramePickler thread")
    while True:
      try:
        frame = self.queue.get()
      except queue.Empty:
        continue
      if frame is None:
        continue
      self.write_frame(frame)