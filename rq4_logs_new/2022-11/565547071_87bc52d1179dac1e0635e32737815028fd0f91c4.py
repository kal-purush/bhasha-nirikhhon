import cv2
from mtcnn.mtcnn import MTCNN
import numpy as np
from PIL import Image,ImageDraw
from keras.models import load_model


#Load the FAcenet Model
def model_load():
    return load_model('facenet_keras.h5')

#Compute the difference
def difference_image(image_array1, image_array2):
    difference = abs(image_array1 - image_array2)
    print((difference.sum()/80)**2)
    return (difference.sum()/80)**2

#Extract face embeddings
def embedding_extractors(faces, model):
    face_embeddings = []
    for i in range(len(faces)):
        pixels = faces[i].astype('float32')
        mean, std = pixels.mean(), pixels.std()
        pixels = (pixels-mean) / std
        samples = np.expand_dims(pixels, axis=0)
        embedding = model.predict(samples)
        face_embeddings.append(embedding[0])
    return face_embeddings

def extract_face_multiple(fileName,detector):  # for multiple person image
    faces = []
    image = Image.open(fileName)
    image = image.convert('RGB')
    pixels = np.asarray(image)
    pixel = detector.detect_faces(pixels)
    # print(pixel)
    for i in range(len(pixel)):
        faces.append(Extract_faces(pixels, pixel, i))
    return (faces, pixel)


def extract_face2(fileName):  # for single person image
    image = Image.open(fileName)
    image = image.convert('RGB')
    pixels = np.asarray(image)
    detector = MTCNN()
    pixel = detector.detect_faces(pixels)
    # print(pixel)
    for i in range(len(pixel)):
        return (Extract_faces(pixels, pixel, i))
        # face_array = asarray(image)
    return np.array([])

def embedding_extractor(fileName, model):  # for single person image testing
    pixels = extract_face2(fileName)
    pixels = pixels.astype('float32')
    mean, std = pixels.mean(), pixels.std()
    pixels = (pixels-mean) / std
    samples = np.expand_dims(pixels, axis=0)
    embedding = model.predict(samples)
    return embedding[0]

def Extract_faces(pixels, pixel, i):
    bgcolor = [255, 255, 255]
    x1, y1, width, height = pixel[i]['box']
    x1, y1 = abs(x1), abs(y1)
    x2, y2 = x1+width, y1+height
    face = pixels[y1:y2, x1:x2]
    image = Image.fromarray(face)
    image = image.resize((160, 160))
    # image.save('test_'+str(i)+'.jpg')
    # image.show()
    return (np.asarray(image)/255)

def extract_face_multiple(frame,detector):  # for multiple person image
    faces = []
    pixels = frame.copy()
    pixel = detector.detect_faces(pixels)
    # print(pixel)
    for i in range(len(pixel)):
        faces.append(Extract_faces(pixels, pixel, i))
    return (faces, pixel)

def play_video(fileName,detector,target_embedding):
    cap = cv2.VideoCapture('video1.mp4')
    video = []
 
# Check if camera opened successfully
    if (cap.isOpened()== False): 
        print("Error opening video stream or file")
    i = 0
    # Read until video is completed
    while(cap.isOpened()):
    # Capture frame-by-frame
        ret, frame = cap.read()
        i = i+1
        print(i)
        if i%25 == 0:
            if ret == True:
                selected_index = -1
                min_distance = np.inf
                face = extract_face_multiple(frame,detector=detector)
                embedded_face = embedding_extractors(face[0], model)
                for i in range(len(embedded_face)):
                    dist = difference_image(target_embedding, embedded_face[i])
                    if dist < min_distance and dist < 1.3:
                        min_distance = dist
                        selected_index = i
                image = Image.fromarray(frame)
                img1 = ImageDraw.Draw(image)

                if selected_index != -1:
                    x1, y1, width, height = face[1][selected_index]['box']
                    x1, y1 = abs(x1), abs(y1)
                    x2,y2 = x1+width,y1+height
                    img1.rectangle([(x1,y1),(x2,y2)], outline=(255, 0, 0), width=8)
                    image.show()
                    video.append(np.array(image))
                # break
                
                
            # Break the loop
            else: 
                break
    for i in range(len(video)):
        try:
            cv2.imshow('Frame',video[i])
            # Press Q on keyboard to  exit
            if cv2.waitKey(25) & 0xFF == ord('q'):
                break
    
        except Exception as e:
            print(e)
    # When everything done, release the video capture object
    cap.release()
    
    # Closes all the frames
    cv2.destroyAllWindows()
model = model_load()
detector = MTCNN()
input_embedding =  embedding_extractor('test_0.jpg', model)
# print(input_embedding)
play_video('',detector,input_embedding)