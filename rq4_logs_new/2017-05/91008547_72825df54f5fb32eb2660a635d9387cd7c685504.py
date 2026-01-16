#importacao de biblioteca
from PIL import Image
import cv2
import numpy 
import math  
 
# Camera 0 is the integrated web cam on my netbook
#A camera 0  esta integrada ao raspberry
camera_port = 0
#numero de frames 
#Number of frames to throw away while the camera adjusts to light levels
ramp_frames = 30
 
# Now we can initialize the camera capture object with the cv2.VideoCapture class.
# All it needs is the index to a camera port.
camera = cv2.VideoCapture(camera_port)
 
# Captures a single image from the camera and returns it in PIL format
def get_image():
 # read is the easiest way to get a full image out of a VideoCapture object.
 retval, im = camera.read()
 return im
nomes_amostras=[]
for i in range(4):
    # Ramp the camera - these frames will be discarded and are only used to allow v4l2
    # to adjust light levels, if necessary
    for i in xrange(ramp_frames):
     temp = get_image()

    # Take the actual image we want to keep
    camera_capture = get_image()
    nome=raw_input("Digite o nome da imagem\n")

    print("Tirando foto...")
    file = "/home/pi/teste/amostras/"+nome+".jpg"
    
    # A nice feature of the imwrite method is that it will automatically choose the
    # correct format based on the file extension you provide. Convenient!
    cv2.imwrite(file, camera_capture)
    print("Foto salva com sucesso")
    nomes_amostras.append(nome)

     
    # You'll want to release the camera, otherwise you won't be able to create a new
    # capture object until your script exits
del(camera)


img_branco_01 = Image.open("/home/pi/teste/amostras/"+"branco.jpg")
half_the_width = img_branco_01.size[0] / 2
half_the_height = img_branco_01.size[1] / 2
img_branco = img_branco_01.crop(
    (
        half_the_width + 23,#esq 
        half_the_height - 25,#cima
        half_the_width + 39,#dir centralizado
        half_the_height +35#baixo nao mexer
    )
)
img_branco.save("recorte_branco.jpg")


#Branco

average_color_per_row_white = numpy.average(img_branco, axis=0)
average_color_white = numpy.average(average_color_per_row_white, axis=0)
#print("--------------\n")
#print("media do Branco\n")
#print "(red,green,blue)"
#print(average_color_white)
average_color_white= numpy.uint8(average_color_white)
average_color_img_white = numpy.array([[average_color_white]*100]*100, numpy.uint8)
cv2.imwrite( "average_color_branco.jpg", average_color_img_white )
media_branco=float(average_color_white[0])/255
media_branco=media_branco+float(average_color_white[1])/255
media_branco=media_branco+float(average_color_white[2])/255
media_branco=media_branco/3



lista_absorbancias=[]
'''nomes_amostras=['r1.jpg','r2.jpg','r3.jpg','r4.jpg']
for i in range(5):
    nome=str(raw_input("digite o nome da imagem com a extencao\n"))
    nomes_amostras.append(nome)'''

for i in range(4):
    img1 = Image.open("/home/pi/teste/amostras/"+nomes_amostras[i]+".jpg")
    half_the_width = img1.size[0] / 2
    half_the_height = img1.size[1] / 2
    img = img1.crop(
        (
            half_the_width + 23,#esq 
            half_the_height - 25,#cima
            half_the_width + 39,#dir centralizado
            half_the_height +35#baixo nao mexer
        )
    )
    img1.close()
    img.save("recorte_"+nomes_amostras[i]+".jpg")


    #Amostra
    average_color_per_row = numpy.average(img, axis=0)
    average_color = numpy.average(average_color_per_row, axis=0)
    average_color = numpy.uint8(average_color)
    average_color_img = numpy.array([[average_color]*100]*100, numpy.uint8)
    cv2.imwrite( "cor_media_"+nomes_amostras[i]+".jpg", average_color_img )


    
    
    #media_amostra
    media_amostra=float(average_color[0])/255
    media_amostra=media_amostra+float(average_color[1])/255
    media_amostra=media_amostra+float(average_color[2])/255
    media_amostra=media_amostra/3
    
    lista_absorbancias.append(-math.log(media_amostra/media_branco))
    #print("++++++++++++++++\n")
    #print("absorbancia do "+nomes_amostras[i])
    #print lista_absorbancias[i]
print("absorbancias\n")
print lista_absorbancias
#.3058804026787278, 0.6207859337411652, 0.813551449696203, 0.9756073090305744]
#assim funciona
concentracao=[0.01,0.02,0.03,0.04]
#concentracao=[0.005,0.02,0.03,0.04]
corr=numpy.corrcoef(lista_absorbancias,concentracao)
print("R2\n")
print(corr)
