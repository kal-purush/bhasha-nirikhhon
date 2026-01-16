from k import ImageProcessor
from PIL import Image, ImageOps
import datetime

now = datetime.datetime.now()

class ImageHandler():
    #функция при создании
    def __init__(self, filename):
        self.filename = filename
        self.img = self.load(f"img\\{self.filename}")
        self.Img = ImageProcessor.ImageProcessor(self.img)
        self.format = self.filename.split('.')[1]
        self.filename = self.filename.split('.')[0]

    #загрузка изображения
    def load(self,filename):
        with Image.open(filename) as img:
            img.load()
        print(f"Изображение {filename} открыто")
        return img
    
    #сохранение изображения
    def save(self):
        self.img = self.Img.img
        self.img.save(f"img\\new_{self.filename.split('.')[0]}.{self.format}")

    #сохранение изображения c датой    
    def save_with_data(self):
        self.img = self.Img.img
        date = (now.strftime("%d-%m-%Y"))
        self.img.save(f'img\\new_{self.filename.split(".")[0]}-{date}.{self.format}')

    #изменение формата
    def change_format(self):
        inp = int(input("1 - jpg\n2 - gif\n3 - png\n4 - jpeg\n"))
        if(inp==1):
            self.format = '.jpg'
        elif(inp==2):
            self.format = '.gif'
        elif(inp==3):
            self.format = '.png'
        elif(inp==4):
            self.format = '.jpeg'
        else:
            print("Некорректный ввод данных")
    
    #масштабирование до 50%
    def change_scale(self):
        self.img = ImageOps.scale(self.img,0.5)


    