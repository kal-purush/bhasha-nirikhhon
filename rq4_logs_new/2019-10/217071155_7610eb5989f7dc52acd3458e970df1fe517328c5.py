# Módulo auxiliar para gerenciar as funções relaciondas a converter imagens em códigos de incorporação

from imutils import paths
import os
from image_helpers import find_face, crop_face, open_crop_and_resize_face, rotate_image
from numpy import asarray
from keras_vggface.vggface import VGGFace
from keras_vggface.utils import preprocess_input

def get_embeddings( database_folder_name, crop=True ):
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
    
    if crop == True:
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