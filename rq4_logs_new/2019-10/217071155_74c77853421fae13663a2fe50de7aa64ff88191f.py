# -*- coding: utf-8 -*-
"""
Código desenvolvido para comparar uma imagem de um rosto( o nome do arquivo deve ser o nome da pessoa)
com uma database já convertida em código de incorporação
e valida se é a mesma pessoa ou não.

@Autor: rodrigo.ferrari
@Editor: Thiago Luis Pinho
"""


import os
import argparse
import cv2
import pickle
from imutils import paths
from numpy import expand_dims
from matplotlib import pyplot
from PIL import Image
from numpy import asarray
from keras_vggface.vggface import VGGFace
from keras_vggface.utils import preprocess_input
from keras_vggface.utils import decode_predictions
from scipy.spatial.distance import cosine
from image_helpers import find_face, crop_face, open_crop_and_resize_face, rotate_image

# Parte do código responsável por criar e configurar os argumentos para a chamada no terminal
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--image", default="database_test",
  help="path to input database of unkown images")
ap.add_argument("-e", "--encodings", default="encodings.pickle",
  help="path to serialized db of facial encodings")
ap.add_argument("-d", "--detection-method", type=str, default="cnn",
  help="face detection model to use: either `hog` or `cnn`")
args = vars(ap.parse_args())

def get_embeddings(database_folder_name,crop=True):
  ''' Recebe uma lista de nomes de arquivos para serem abertos. Caso crop = True, ele antes de calcular
  o embedding tenta encontrar o rosto e retorna uma imagem com somente ele. 
  Retorna uma lista de códigos de incorporação e uma lista dos nomes correspondentes desses códigos'''
  # Pega o caminho para a database passada no argumento
  print("[INFO] Contando rostos em " + database_folder_name + " ...")
  imagePaths = list(paths.list_images( database_folder_name ))
  faces = []
  filenames = []
  # Itera sobre os caminhos das imagens
  for (i, imagePath) in enumerate(imagePaths):
    # Extrai a face da pessoa na imagem, caso haja
    print("[INFO] Processando imagem {}/{}".format(i + 1,
      len(imagePaths)))

    filename = imagePath.split(os.path.sep)[-1]
    filename = filename[:-5]
    filenames.append(filename)
    
    if crop==True:
      # Se os rostos precisarem ser cortados das fotos, ele o faz.
      face = open_crop_and_resize_face(imagePath)
      faces.append(face)
    else:
      # Caso contrário ele somente abre e muda para o tamanho adequado.
      face = pyplot.imread(imagePath)
      face = cv2.resize(face, (224,224) )
      faces.append(face)

    #  Converte em um array de samples
  samples = asarray(faces, 'float32')
  # Faz os preprocessamentos necessários nas imagens para elas entrarem no modelo. Ex: Centraliza o rosto
  samples = preprocess_input(samples, version=2)
  # Cria um modelo já treinado do VGGFace
  model = VGGFace(model='resnet50', include_top=False, input_shape=(224, 224, 3), pooling='avg')
  # Aplica o modelo sobre as imagens e retorna um vetor de códigos de incorporação
  embeddings = model.predict(samples)

  return embeddings, filenames

# determine if a candidate face is a match for a known face
def is_match(known_embedding, candidate_embedding, thresh=0.5):
  # calculate distance between embeddings
  score = cosine(known_embedding, candidate_embedding)
  if score <= thresh:
    print('>face is a Match (%.3f <= %.3f)' % (score, thresh))
  else:
    print('>face is NOT a Match (%.3f > %.3f)' % (score, thresh))

print("[INFO] loading encodings...")
data = pickle.loads(open(args["encodings"], "rb").read())

unknown_encodings, unknown_names = get_embeddings(args["image"])

# initialize the list of names for each face detected
person_names = []
person_encodings = []

person_encodings = data[unknown_names[0]]

# loop over the facial embeddings
for encoding in unknown_encodings:
  # attempt to match each face in the input image to our known
  # encodings
  for known_encoding in person_encodings:
      is_match(encoding, known_encoding)
      """  
  if True in matches:
    # loop over the recognized faces
    for ((top, right, bottom, left), name) in zip(boxes, person_names):
      # draw the predicted face name on the image
      cv2.rectangle(resized, (left, top), (right, bottom), (0, 255, 0), 2)
      y = top - 15 if top - 15 > 15 else top + 15
      cv2.putText(resized, name, (left, y), cv2.FONT_HERSHEY_SIMPLEX, 0.75, (0, 255, 0), 2) """

# show the output image
#cv2.imshow("Image", resized)
#cv2.waitKey(0)