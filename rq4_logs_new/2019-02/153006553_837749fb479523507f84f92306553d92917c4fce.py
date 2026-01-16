import cv2
import utils
from image_resizer import ImageResizer
from video_capture import VideoCapture
from cv_named_window import CvNamedWindow
from video_controller import VideoController
import video_sources


class MotionState:
    # Started = 1
    InProgress = 2
    # Ended = 3
    Stable = 4
    __descriptions = ['', 'Started', 'InProgress', 'Ended', 'Stable']

    @staticmethod
    def describe(state):
        return MotionState.__descriptions[state]


class Motion:
    def __init__(self, frame0, map_threshold=45, min_px_to_stable=10):
        self.stable_frame = self.prev_frame = self.denoise(frame0)
        self.map_threshold = map_threshold
        self.state = MotionState.Stable
        self.foreground_mask = None
        self.min_px_to_stable = min_px_to_stable

    def update(self, current_frame):
        self.foreground_mask = self.__foreground_mask(self.denoise(current_frame), self.prev_frame, self.map_threshold,
                                                      dst=self.foreground_mask)

        non_zero = cv2.countNonZero(self.foreground_mask)
        motion_detected = non_zero > self.min_px_to_stable

        if self.state == MotionState.Stable:
            if motion_detected:
                self.state = MotionState.InProgress
        elif self.state == MotionState.InProgress:
            if not motion_detected:
                self.state = MotionState.Stable

        self.prev_frame = current_frame

    @staticmethod
    def __foreground_mask(current_frame, prev_frame, threshold, dst=None):
        diff = cv2.absdiff(prev_frame, current_frame)
        mask = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY, dst=dst)
        _, mask = cv2.threshold(mask, threshold, 255, cv2.THRESH_BINARY, dst=mask)
        return mask

    @staticmethod
    def denoise(frame):
        return cv2.GaussianBlur(frame, (3, 3), 0, dst=frame)


def main():
    video = VideoCapture(video_sources.video_2)
    vc = VideoController(1, state='pause')
    resizer = ImageResizer(video.resolution(), scale=0.45)

    wnd = CvNamedWindow('video')
    mask_wnd = CvNamedWindow('mask')
    changes_wnd = CvNamedWindow('changes')

    motion = Motion(resizer.resize(video.read()))

    for frame in video.frames():

        small_frame = resizer.resize(frame)
        prev_state = motion.state
        motion.update(small_frame.copy())
        current_state = motion.state

        if prev_state == MotionState.InProgress and current_state == MotionState.Stable:
            denoised_frame = Motion.denoise(small_frame)
            diff = cv2.absdiff(motion.stable_frame, denoised_frame)
            mask = cv2.cvtColor(diff, cv2.COLOR_BGR2GRAY)
            _, mask = cv2.threshold(mask, 45, 255, cv2.THRESH_BINARY)
            mask = cv2.cvtColor(mask, cv2.COLOR_GRAY2BGR, dst=diff)

            state_changes = cv2.bitwise_and(small_frame, mask)
            # changes_wnd.imshow(state_changes)
            # cv2.waitKey(1)
            motion.stable_frame = denoised_frame

        wnd.imshow(small_frame)
        mask_wnd.imshow(motion.foreground_mask)

        if vc.wait_key() == 27: break

    video.release()
    wnd.destroy()


if __name__ == '__main__':
    main()