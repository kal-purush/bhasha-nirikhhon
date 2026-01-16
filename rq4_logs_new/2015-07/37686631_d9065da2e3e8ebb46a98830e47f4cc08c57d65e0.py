# -*- coding: utf-8 -*-
from function import image_path
from function import data_path
from skimage import io
from skimage import measure
from skimage import morphology
from skimage import filter
import numpy as np
import math
import os


class Caracteristica(object):

    def __image_list(self, *paths):
        print paths
        images = []
        for path in paths:
            walk_gen = os.walk(path)
            for root, dirs, files in walk_gen:
                for f in files:
                    if (f.endswith('.jpg') or f.endswith('.jpeg')):
                        images.append(os.sep.join([root, f]))

        return images

    def __features(self, image_path):

        img = io.imread(image_path, True)
        img_otsu = filter.threshold_otsu(img) #Aplicar otsu para binarizar a imagem
        img = img < img_otsu

        # Find contours at a constant value of 0.5
        contours = measure.find_contours(img, 0.5)

        arclen=0.0
        for n, contour in enumerate(contours):
            arclenTemp=0.0
            for indice, valor in enumerate(contour):
                if indice > 0:
                    d1 = math.fabs(round(valor[0]) - round(contour[indice-1,0]))
                    d2 = math.fabs(round(valor[1]) - round(contour[indice-1,1]))
                    if d1+d2>1.0:
                        arclenTemp+=math.sqrt(2)
                    elif d1+d2 == 1:
                        arclenTemp+=1
            if arclenTemp > arclen:
                arclen = arclenTemp
                bestn = n
            
        #Transforma a lista contours[bestn] em uma matriz[n,2]
        aux = np.asarray(contours[bestn])

        #---------------------------  Curvatura --------------
        vetor = [] #vetor que irá receber as curvaturas k(t)
    
        for i in range(len(aux)-2):
            #---------------------------  Curvatura --------------
    
            b1 = ( (aux[i-2,1]+aux[i+2,1]) + (2*(aux[i-1,1] + aux[i+1,1])) - (6*aux[i,1]) ) / 12
            b2 = ( (aux[i-2,0]+aux[i+2,0]) + (2*(aux[i-1,0] + aux[i+1,0])) - (6*aux[i,0]) ) / 12
            c1 = ( (aux[i+2,1]-aux[i-2,1]) + (4*(aux[i+1,1] - aux[i-1,1])) ) / 12
            c2 = ( (aux[i+2,0]-aux[i-2,0]) + (4*(aux[i+1,0] - aux[i-1,0])) ) / 12

            k = (2*(c1*b1 - c2*b2)) / ((c1**2 + c2**2)**(3/2))

            vetor.append(k) #append: insere objeto no final da lista    

        classe = image_path.split(os.sep)[len(image_path.split(os.sep))-2]
        return img, vetor, arclen, classe


    def run(self):

        print 'Gerando Caracteristicas....'
        images = self.__image_list(image_path('circinatum'), image_path('kelloggii'), image_path('negundo'))

        arquivo = open(data_path(),'w')
        count = 0
        for i in images:
            count+=1
            img, vetor, arclen, classe = self.__features(i)

            # Media Curvatura
            arquivo.write(str(np.mean(np.abs(vetor))) + ',')
            # Comprimento de Arco
            arquivo.write(str(arclen) + ',')            
            # Area
            arquivo.write(str(np.sum(img)) + ',')          
            # Numero Pixels Esqueleto
            arquivo.write(str(np.sum(morphology.medial_axis(img))) + ',')
            # Classe Folhas
            arquivo.write(classe)
            arquivo.write("\n")

        arquivo.close()

        print '100%'
        print 'Total Imagens: ' + str(count)


if __name__ == '__main__':
    Caracteristica().run()