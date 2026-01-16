# Imports
import cv2
import numpy as np


class Application:

    """
    Class for

    Parameters
    ----------
    model : trainer.Model
        Object that interfaces with our model
    util : data_util.ImageProcessor
        Object that interfaces with the data

    """

    def __init__(self, model, util):
        # Set up the model
        self.model = model
        self.model.load_model()

        # Set up the data generator
        self.util = util

    def run(self):
        """
        Run the application.

        """
        # Initialize the data pipeline
        grabber = self.util.grabber()
        pipeline = self.util.image_pipeline(grabber, self.model.MODEL_IMAGE_SIZE)

        # Initialize the window
        cv2.namedWindow("Frame", cv2.WND_PROP_FULLSCREEN)
        cv2.setWindowProperty("Frame", cv2.WND_PROP_FULLSCREEN, cv2.WINDOW_FULLSCREEN)

        # Launch the application
        while True:
            # Get the frame and eye coordinates
            img, eyes, inputs = next(pipeline)
            prediction = self.model.predict(inputs)

            # Draw the eye rectangles
            # img = np.uint8(255 * img)
            img = cv2.cvtColor(img, cv2.COLOR_GRAY2BGR)
            for rect in eyes:
                pt1 = tuple(rect[:2])
                pt2 = tuple(np.add(rect[:2], rect[2:]))
                cv2.rectangle(img, pt1, pt2, [255, 0, 0], 1)

            # Prepare the frame
            frame = np.zeros((self.util.SCREEN_HEIGHT, self.util.SCREEN_WIDTH, 3), dtype=np.uint8)
            top, left = np.subtract(frame.shape[:2], img.shape[:2]) // 2
            bottom, right = np.add((top, left), img.shape[:2])
            frame[top:bottom, left:right, :] = img

            # Write the prediction
            cv2.putText(
                frame, str(prediction), (100, 100),
                fontFace=cv2.FONT_HERSHEY_SIMPLEX, fontScale=2,
                color=[0, 255, 0]
            )

            cv2.imshow("Frame", frame)
            if cv2.waitKey(1) == ord("q"):
                break