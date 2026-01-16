import pyqrcode
import pandas as pd


def createQRCode():
    df = pd.read_csv("data.csv")
    for index, values in df.iterrows():
        name = values["Name"]
        price = values["Price"]


        data = f'''
        Name: {name} \n
        Price: {price}
        '''
        image = pyqrcode.create(data)
        image.svg(f"{name}", scale="7")


createQRCode()