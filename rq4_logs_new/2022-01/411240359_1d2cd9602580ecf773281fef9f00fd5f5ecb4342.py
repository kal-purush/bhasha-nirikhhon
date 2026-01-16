import mediapipe as mp
import cv2
import os
import utils

images = {} #hold the images dictionary

LIPS_LANDMARKS = [ 61,
                  146,
                  91,
                  181,
                  84,
                  17,
                  314,
                  405,
                  321,
                  375,
                  291,
                  185,
                  40,
                  39,
                  37,
                  0,
                  267,
                  269,
                  270,
                  409,
                  78,
                  95,
                  88,
                  178,
                  87,
                  14,
                  317,
                  402,
                  318,
                  324,
                  308,
                  191,
                  80,
                  81,
                  82,
                  13,
                  312,
                  311,
                  310,
                  415]

MY_FACE_CONNECTIONS = frozenset([
    # Lips.
    (61, 146),
    (146, 91),
    (91, 181),
    (181, 84),
    (84, 17),
    (17, 314),
    (314, 405),
    (405, 321),
    (321, 375),
    (375, 291),
    (61, 185),
    (185, 40),
    (40, 39),
    (39, 37),
    (37, 0),
    (0, 267),
    (267, 269),
    (269, 270),
    (270, 409),
    (409, 291),
    (78, 95),
    (95, 88),
    (88, 178),
    (178, 87),
    (87, 14),
    (14, 317),
    (317, 402),
    (402, 318),
    (318, 324),
    (324, 308),
    (78, 191),
    (191, 80),
    (80, 81),
    (81, 82),
    (82, 13),
    (13, 312),
    (312, 311),
    (311, 310),
    (310, 415),
    (415, 308)
])


# Load drawing_utils and drawing_styles
mp_drawing = mp.solutions.drawing_utils 
mp_drawing_styles = mp.solutions.drawing_styles
mp_face_mesh = mp.solutions.face_mesh

circleDrawingSpec = mp_drawing.DrawingSpec(thickness=1, circle_radius=1, color=(0,255,0))
videoFolderPath = 'C:\\Users\\212574830\\Desktop\\Project symmetry\\\TestVideos'

#debug code - run code on images from disk insead of exraction from video
if 0:
  videoFolderPath = 'C:\\Users\\212574830\\Desktop\\Project symmetry\\Videos\\Movement_sense_video'
  utils.load_images_from_folder('C:\\Users\\212574830\\Desktop\\Project symmetry\\Videos\\Movement_sense_video\\Images')

utils.extractImagesFromVideo(videoFolderPath, images, False)

# Preview the images.
for name, image in images.items():
  print(name)   
  utils.resize_and_show(image)


#Run FaceMesh on each image to get landmarks
with mp_face_mesh.FaceMesh(
    static_image_mode=False,        #Was True, unrelated images (True) or as a video stream (False)
    max_num_faces=2,
    min_detection_confidence=0.5) as face_mesh:

  for name, image in images.items():
    # Convert the BGR image to RGB and process it with MediaPipe Face Mesh.
    results = face_mesh.process(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))

    #get the image relevant (X,Y) points, hashmap of {idx: (X,Y)}
    landmarkImagePoints = utils.getFilteredLandmarkData(results.multi_face_landmarks[0], LIPS_LANDMARKS)

    # Draw face landmarks of each face.
    print(f'Face landmarks of {name}:')
    if not results.multi_face_landmarks:
      continue

    annotated_image = image.copy()
    #cv2.imshow('',annotated_image)

    #create image landmarks output folder
    OutPath = 'C:\\Users\\212574830\\Desktop\\Project symmetry\\TestImages'
    os.makedirs(OutPath,exist_ok=True)

    for face_landmarks in results.multi_face_landmarks:

      #Draw Mesh
      if(1):
          mp_drawing.draw_landmarks(
          image=annotated_image,
          landmark_list=face_landmarks,
          connections=mp_face_mesh.FACEMESH_TESSELATION,
          landmark_drawing_spec=None,
          connection_drawing_spec=mp_drawing_styles
          .get_default_face_mesh_tesselation_style())

      #draw fram lines
      if(0):
        mp_drawing.draw_landmarks(
        image=annotated_image,
        landmark_list=face_landmarks,
        connections=MY_FACE_CONNECTIONS ,
        landmark_drawing_spec=circleDrawingSpec,#None,
        connection_drawing_spec=mp_drawing_styles
        .get_default_face_mesh_contours_style())

      utils.resize_and_show(annotated_image)    

      #degug: print landmark markers + numbering on image.
      #utils.printLandmarkPoints(landmarkImagePoints, annotated_image)

      #find and print landmarks center
      cetner = utils.centerMass(landmarkImagePoints)
      annotated_image = cv2.drawMarker(annotated_image, ((int)(cetner['X']), (int)(cetner['Y'])) , (255, 0, 0), 0, 10)

      #find and print face reference line
      refSrc = utils.convertPoint(results.multi_face_landmarks[0], 9)
      refDst = utils.convertPoint(results.multi_face_landmarks[0], 200)
      utils.drawLineOnImage(annotated_image, refSrc, refDst)

      if(0):
        cv2.imshow('', annotated_image)

      # addd text to Image
      #imageWithText = addTextOnImage(annotated_image,'Hello World')
      #cv2.imwrite(os.path.join(OutPath, name + '.jpg'), imageWithText)
        
      cv2.imwrite(os.path.join(OutPath, name + '.jpg'), annotated_image)  



          