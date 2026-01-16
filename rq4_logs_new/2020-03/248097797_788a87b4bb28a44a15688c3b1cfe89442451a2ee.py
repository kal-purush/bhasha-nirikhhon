import os
import json
from time import sleep
from shutil import move
from pathlib import Path

class App:
    def __init__(self):
        with open('folder', 'r') as f:
            self.folder = Path(f.read())
            f.close()

        with open('data.json', 'r') as f:
            self.data = json.load(f)
            f.close()

        self.organize(self.folder)

    def organize(self, folder):
        files = os.listdir(folder)
        if len(files) != 0:
            for file in files:
                ex = file.split('.')[-1]
                for i in self.data:
                    if ex in i['ext']:
                        try:
                            move(folder / file, folder / f'{i["name"]}/{file}')
                        except FileNotFoundError:
                            os.mkdir(folder / i['name'])
                            move(folder / file, folder / f'{i["name"]}/{file}')
                        break

if __name__ == '__main__':
    App()