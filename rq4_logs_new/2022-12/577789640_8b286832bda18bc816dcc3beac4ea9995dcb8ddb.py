import mediapipe as mp
import cv2
import shutil
import os
import numpy as np


def findBrokenFile():
    mp_drawing = mp.solutions.drawing_utils
    mp_drawing_styles = mp.solutions.drawing_styles
    mp_hands = mp.solutions.hands

    letters = ["A", "B", "C", "D", "del", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "nothing", "O", "P", "Q",
               "R",
               "S", "space", "T", "U", "V",
               "W", "X", "Y", "Z"]
    IMAGE_FILES = []
    BROKEN_FILES = []
    tempWord = "C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data" \
               "\\Letters\\asl_alphabet_train\\asl_alphabet_train"
    for letter in letters:
        for i in range(3000):
            tempWord = tempWord + "\\" + letter + "\\" + letter + str((i + 1)) + ".jpg"
            IMAGE_FILES.append(tempWord)
            tempWord = "C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data" \
                       "\\Letters\\asl_alphabet_train\\asl_alphabet_train"

    with mp_hands.Hands(
            static_image_mode=True,
            max_num_hands=2,
            min_detection_confidence=0.5) as hands:
        for idx, file in enumerate(IMAGE_FILES):

            image = cv2.flip(cv2.imread(file), 1)
            results = hands.process(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
            # fotografin kutuphane tarafindan algilanip algilanmadigi kontrolu
            if not results.multi_hand_landmarks:
                temp = file.split("\\")
                BROKEN_FILES.append(temp[10])
                print(file)
                continue
    # okunamayan resimlerin kaydının tutulması
    with open("BROKEN_FILES.txt", "w") as f:
        for line in BROKEN_FILES:
            f.write(line)
            f.write("\n")


# findBrokenFile()


def copyBrokenFile():
    fileSrc = "C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data\\Letters\\asl_alphabet_train\\asl_alphabet_train"
    fileDrc = "C:\\Users\\samet\\Desktop\\bitirme-projesi\\brokenFiles"
    with open("BROKEN_FILES.txt", "r") as f:
        brokenArray = f.read().splitlines()
        for line in brokenArray:
            src = fileSrc + "\\" + line
            shutil.copy(src, fileDrc)


# copyBrokenFile()

def copyBrokenFileToDirectory():
    fileSrc = "C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data\\Letters\\asl_alphabet_train\\asl_alphabet_train"
    FILE_PATH = "C:\\Users\samet\Desktop\\bitirme-projesi\BrokenData\\"
    with open("OPTIMIZED_FILES.txt", "r") as f:
        brokenArray = f.read().splitlines()
        for line in brokenArray:
            if "Folder Name" in line:
                folder = line.split(":")[1]
                src = f"{FILE_PATH}\\{folder}\\"
                os.makedirs(os.path.dirname(src), exist_ok=True)
                continue
            imgSource = f"{fileSrc}\\{folder}\\{line}"
            shutil.copy(imgSource, FILE_PATH + folder)


# copyBrokenFileToDirectory()


def optimizeFile():
    with open("BROKEN_FILES.txt", "r") as f:
        tempArray = f.read().splitlines()
        newArray = []
        temp = ""
        for line in tempArray:
            for char in line:
                if char.isdigit():
                    if temp != line.split(char)[0]:
                        temp = line.split(char)[0]
                        newArray.append("Folder Name:" + temp)
                    break
            newArray.append(line)
        print(newArray)
    with open("OPTIMIZED_FILES.txt", "w") as x:
        for line in newArray:
            x.write(line)
            x.write("\n")


# optimizeFile()


def deleteBrokenFiles():
    with open("OPTIMIZED_FILES.txt", "r") as f:
        tempArray = f.read().splitlines()
        for line in tempArray:
            if "Folder Name" in line:
                folder = line.split(":")[1]
                continue
            os.remove(
                f"C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data\\Letters\\asl_alphabet_train\\asl_alphabet_train\\{folder}\\{line}")


# deleteBrokenFiles()

def OPTIMIZE_FILES():
    letters = ["A", "B", "C", "D", "del", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "nothing", "O", "P", "Q",
               "R",
               "S", "space", "T", "U", "V",
               "W", "X", "Y", "Z"]
    with open("PICTURES.txt", "w") as f:
        for harf in letters:
            pictures = os.listdir(
                f"C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data\\Letters\\asl_alphabet_train\\asl_alphabet_train\\{harf}\\")
            tempList = []
            for pic in pictures:
                x = pic.split(harf)[1]
                ekle = x.split(".")[0]
                tempList.append(ekle)

            print(tempList)
            tempList = list(map(int, tempList))
            tempList.sort()

            print(tempList)
            f.write(f"FolderName:{harf}")
            f.write("\n")
            for eleman in tempList:
                tempString = harf + str(eleman) + ".jpg"
                f.write(tempString)
                f.write("\n")


# OPTIMIZE_FILES()


def findSimilarFile():
    mp_hands = mp.solutions.hands
    with open("PICTURES.txt", "r") as f:
        pictures = f.read().splitlines()
    letter = ""
    IMAGE_FILES = []
    for picture in pictures:
        if "FolderName" in picture:
            letter = picture.split(":")[1]
            IMAGE_FILES.append(f"FolderName:{letter}")
            continue

        IMAGE_FILES.append(
            f"C:\\Users\\samet\\Desktop\\bitirme-projesi\\Data\\Letters\\asl_alphabet_train\\asl_alphabet_train\\"
            f"{letter}\\{picture}")
    print(IMAGE_FILES)
    with open("SIMILARS.txt", "w") as f:
        with mp_hands.Hands(
                static_image_mode=True,
                max_num_hands=2,
                min_detection_confidence=0.5) as hands:
            boolSimilar = True
            for idx, file in enumerate(IMAGE_FILES):
                print(boolSimilar)
                if "FolderName" in file:
                    temp = file.split(":")[1]
                    f.write(f"FolderName:{temp}")
                    f.write("\n")
                    continue
                if "FolderName" in IMAGE_FILES[idx+1]:
                    continue

                image = cv2.flip(cv2.imread(file), 1)
                results = hands.process(cv2.cvtColor(image, cv2.COLOR_BGR2RGB))
                image2 = cv2.flip(cv2.imread(IMAGE_FILES[idx + 1]), 1)
                results2 = hands.process(cv2.cvtColor(image2, cv2.COLOR_BGR2RGB))

                if results.multi_hand_landmarks == results2.multi_hand_landmarks:
                    continue
                if boolSimilar:
                    tempPictureName = file.split("\\")[10]
                    f.write("----------------------------------------------")
                    f.write(f"{tempPictureName} fotografina benzeyenler:\n")
                    hand1 = np.array(
                        [[res.x, res.y, res.z] for res in results.multi_hand_landmarks[0].landmark]).flatten()

                hand2 = np.array([[res.x, res.y, res.z] for res in results2.multi_hand_landmarks[0].landmark]).flatten()
                sayac = 0
                benzerlik = 55
                for i in range(63):
                    if i == 62:
                        if sayac < benzerlik:
                            boolSimilar = True

                    fark = abs(hand1[i] - hand2[i])


                    if fark < 0.035:
                        if sayac >= benzerlik:
                            print(tempPictureName, "fotografı", IMAGE_FILES[idx + 1].split("\\")[10],
                                  "fotografına benziyor")
                            benzeyen = IMAGE_FILES[idx + 1].split("\\")[10]
                            f.write(f"{benzeyen}\n")
                            boolSimilar = False
                            break

                        sayac += 1


# findSimilarFile()