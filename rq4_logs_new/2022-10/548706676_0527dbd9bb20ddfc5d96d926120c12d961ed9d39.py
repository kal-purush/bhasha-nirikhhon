from PIL import Image, ImageChops, ImageOps, ImageEnhance

def main():
    imagen = Image.open("lord.webp")

    #Invertir colores
    nuevaImagen = ImageChops.invert(imagen)
    nuevaImagen.save("InvertirColores.jpg")

    #Escala de Grises
    nuevaImagen = ImageOps.grayscale(imagen)
    nuevaImagen.save("escalaGrises.jpg")

    #Cambiar tamaño
    nuevaImagen = nuevaImagen.resize((320,240))
    nuevaImagen.save("tamanio.jpg")

main()