import os

def build_image() -> None:
    os.system('docker build -t nullboard_img .')

if __name__ == "__main__":
    build_image()