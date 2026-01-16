arr1 = ['а', 'б', 'в', 'г', 'д', 'е', 'ё', 'ж', 'з', 'и', 'й', 'к', 'л', 'м', 'н', 'о', 'п', 'р', 'с', 'т', 'у',
        'ф', 'х', 'ц', 'ч', 'ш', 'щ', 'ъ', 'ы', 'ь', 'э', 'ю', 'я']

arr2 = arr1.copy()

arr3 = ['\'', '—', ']', '~', '}', '⟨', '―', '{', '⟩', ':', ',', '‒', '…', '!', '–', '.',
        '-', '?', '«', ')', '>', '\"', '[', '‐', ';', '/', '&', '|', '<', '»', '=', '_', '+', '*', '^', '\', '№', '$', '#', '%', '@', '(']

arr4 = arr3.copy()


def change_arr2():
    for i in range(3):
        arr2.append(arr2[0])
        arr2.remove(arr2[0])
        arr4.append(arr4[0])
        arr4.remove(arr4[0])

def encrypt():
    with open("input.txt", "r") as file:
        contents = file.read()
        contents = contents.lower()
        contentse = ""
        change_arr2()
        for i in contents:
            trigger = False
            for j in range(len(arr1)):
                if i == arr1[j]:
                    contentse += arr2[j]
                    trigger = True
            if not trigger:
                for j in range(len(arr3)):
                    if i == arr3[j]:
                        contentse += arr4[j]
                        trigger = True
                if not trigger:
                    contentse += i
    with open("output.txt", "w") as file:
        file.write(contentse)


def decrypt():
    with open("input.txt", "r") as file:
        contents = file.read()
        contents = contents.lower()
        contentse = ""
        change_arr2()
        for i in contents:
            trigger = False
            for j in range(len(arr1)):
                if i == arr2[j]:
                    contentse += arr1[j]
                    trigger = True
            if not trigger:
                for j in range(len(arr3)):
                    if i == arr4[j]:
                        contentse += arr3[j]
                        trigger = True
                if not trigger:
                    contentse += i
    with open("output.txt", "w") as file:
        file.write(contentse)


print("Выберите действие: \n 1 - Зашифровать сообщение \n 2 - Расшифровать сообщение")
a = int(input())
if a == 1:
    encrypt()
elif a == 2:
    decrypt()
else:
    print('Напишите цифру "1" или "2"')