import mediapipe as mp
from mediapipe.tasks import python
from mediapipe.tasks.python import vision
import numpy as np
import cv2 as cv
import time

#@markdown We implemented some functions to visualize the hand landmark detection results. <br/> Run the following cell to activate the functions.

from mediapipe import solutions
from mediapipe.framework.formats import landmark_pb2
import numpy as np

MARGIN = 10  # pixels
FONT_SIZE = 1
FONT_THICKNESS = 1
HANDEDNESS_TEXT_COLOR = (88, 205, 54) # vibrant green

RESULT = None

def draw_landmarks_on_image(rgb_image, detection_result):
  if not detection_result:
    pass
  hand_landmarks_list = detection_result.hand_landmarks
  handedness_list = detection_result.handedness
  annotated_image = np.copy(rgb_image)

  # Loop through the detected hands to visualize.
  for idx in range(len(hand_landmarks_list)):
    hand_landmarks = hand_landmarks_list[idx]
    handedness = handedness_list[idx]

    # Draw the hand landmarks.
    hand_landmarks_proto = landmark_pb2.NormalizedLandmarkList()
    hand_landmarks_proto.landmark.extend([
      landmark_pb2.NormalizedLandmark(x=landmark.x, y=landmark.y, z=landmark.z) for landmark in hand_landmarks
    ])
    solutions.drawing_utils.draw_landmarks(
      annotated_image,
      hand_landmarks_proto,
      solutions.hands.HAND_CONNECTIONS,
      solutions.drawing_styles.get_default_hand_landmarks_style(),
      solutions.drawing_styles.get_default_hand_connections_style())

    # Get the top left corner of the detected hand's bounding box.
    height, width, _ = annotated_image.shape
    x_coordinates = [landmark.x for landmark in hand_landmarks]
    y_coordinates = [landmark.y for landmark in hand_landmarks]
    text_x = int(min(x_coordinates) * width)
    text_y = int(min(y_coordinates) * height) - MARGIN

    # Draw handedness (left or right hand) on the image.
    cv.putText(annotated_image, f"{handedness[0].category_name}",
                (text_x, text_y), cv.FONT_HERSHEY_DUPLEX,
                FONT_SIZE, HANDEDNESS_TEXT_COLOR, FONT_THICKNESS, cv.LINE_AA)

  return annotated_image

model_path = 'Model/hand_landmarker.task'

BaseOptions = mp.tasks.BaseOptions
HandLandmarker = mp.tasks.vision.HandLandmarker
HandLandmarkerOptions = mp.tasks.vision.HandLandmarkerOptions
HandLandmarkerResult = mp.tasks.vision.HandLandmarkerResult
VisionRunningMode = mp.tasks.vision.RunningMode

# Create a hand landmarker instance with the live stream mode:
# this function will output the results of the model
def print_result(result: HandLandmarkerResult, output_image: mp.Image, timestamp_ms: int):
    #print('hand landmarker result: {}'.format(result))
    global RESULT 
    RESULT = result

# these are the parameters for the media pipe model
options = HandLandmarkerOptions(
    base_options=BaseOptions(model_asset_path=model_path),
    running_mode=VisionRunningMode.LIVE_STREAM,
    result_callback=print_result)

with HandLandmarker.create_from_options(options) as landmarker:
    # The landmarker is initialized in the with block.
    #open cv biggins making calls to webcam
    cap = cv.VideoCapture(0)
    if not cap.isOpened():
        print("cannot open camera")
        exit()
    while True:
        # this is responsible for capturing each frame
        ret, frame = cap.read()
        # assuming the frame was read correctly then ret should be True
        if not ret:
            print("can't recieve frame")
            break
        
        #converts the cv2 capture to the media pipe format
        mp_image = mp.Image(image_format=mp.ImageFormat.SRGB, data=frame)

        #this sends that data to the model for markinh
        landmarker.detect_async(mp_image, mp.Timestamp.from_seconds(time.time()).value)
        #since this is using the media pipe live stream for results are returned
        # via result_callback by storing it into the RESULT global variable
        if RESULT:
            print(RESULT.hand_landmarks)
            annotated_image = draw_landmarks_on_image(mp_image.numpy_view(), RESULT)
            cv.imshow('frame', annotated_image)      
        else:
            cv.imshow("frame", frame)

        if cv.waitKey(1) == ord("q"):
            break
cap.release()
cv.destroyAllWindows()