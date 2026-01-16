# -*- coding: utf-8 -*-
"""
Código desenvolvido para converter uma database de imagens em códigos de incorporações
que depois podem ser comparados a fim de validar se um rosto pertence a uma pessoa 
ou não.

@Autor: rodrigo.ferrari
@Editor: Thiago Luis Pinho
"""


import keras_vggface
# Importa a rede neural já treinada que gera os códigos de incorporação.
# Caso você queira saber a versão, basta descomentar o código abaixo.
# print(keras_vggface.__version__),

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
from image_helpers import find_face, crop_face, crop_and_resize_face, rotate_image

# Parte do código responsável por criar e configurar os argumentos para a chamada no terminal
ap = argparse.ArgumentParser()
ap.add_argument("-db", "--database", default="database_labeled",
  help="path to input directory of faces + images")
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

    filename = imagePath.split(os.path.sep)[-2]
    filenames.append(filename)
    if crop==True:
      # Se os rostos precisarem ser cortados das fotos, ele o faz.
      face = crop_and_resize_face(imagePath)
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

# Converte as imagens da datavase em códigos de incorporação
embbedings, filenames = get_embeddings(args["database"])


# Itera sobre os códigos de incorporações
data = {}
for embbeding, filename in zip(embbedings, filenames):
  # Adiciona cada nome e código a listas correspondentes.
  if filename in data:
    data[filename].append( embbeding )
  else: 
    data[filename] = [embbeding]
  

# Armazena os nomes e códigos gerados
f = open(args["encodings"], "wb")
f.write(pickle.dumps(data))
f.close()