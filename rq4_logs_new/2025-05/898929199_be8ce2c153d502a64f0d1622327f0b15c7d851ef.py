import os
import cv2
import face_recognition
from typing import List, Dict
import numpy as np
import logging
logger = logging.getLogger(__name__)

class FaceVerifier:
    """
    Service to load known faces and verify presence in uploaded videos.
    """
    def __init__(self, database_dir: str, tolerance: float = 0.6, num_frames: int = 5):
        self.database_dir = database_dir
        self.tolerance = tolerance
        self.num_frames = num_frames
        self.known_encodings: List[np.ndarray] = []
        self.names: List[str] = []
        if not os.path.isdir(self.database_dir):
            os.makedirs(self.database_dir, exist_ok=True)
        self._load_database()

    def _load_database(self):
        # Load each image in the database directory and compute its encoding
        for filename in os.listdir(self.database_dir):
            filepath = os.path.join(self.database_dir, filename)
            if not os.path.isfile(filepath):
                continue
            try:
                image = face_recognition.load_image_file(filepath)
                encodings = face_recognition.face_encodings(image)
                if encodings:
                    self.known_encodings.append(encodings[0])
                    name, _ = os.path.splitext(filename)
                    self.names.append(name)
            except Exception:
                # Skip files that cannot be processed
                continue

    def verify_video(self, video_path: str) -> Dict[str, object]:
        # Sample a few frames evenly and compare face encodings
        video_capture = cv2.VideoCapture(video_path)
        frame_count = int(video_capture.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
        if frame_count == 0:
            video_capture.release()
            return {"verified": False, "recognized_names": []}
        indices = np.linspace(0, frame_count - 1, self.num_frames, dtype=int)
        recognized = set()
        for idx in indices:
            video_capture.set(cv2.CAP_PROP_POS_FRAMES, idx)
            ret, frame = video_capture.read()
            if not ret:
                continue
            rgb_frame = frame[:, :, ::-1]
            # let the library detect faces and compute encodings
            try:
                encs = face_recognition.face_encodings(rgb_frame)
            except Exception as e:
                logger.warning(f"Face encoding failed on frame {idx}: {e}")
                continue
            for enc in encs:
                matches = face_recognition.compare_faces(self.known_encodings, enc, tolerance=self.tolerance)
                if True in matches:
                    first_match = matches.index(True)
                    recognized.add(self.names[first_match])
        video_capture.release()
        return {"verified": bool(recognized), "recognized_names": list(recognized)}