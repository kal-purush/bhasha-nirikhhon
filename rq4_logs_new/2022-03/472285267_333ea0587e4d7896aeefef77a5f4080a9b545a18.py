if __name__ == '__main__':
    print("Introduce dos números enteros")
    minNum = int(input())
    maxNum = int(input())

    if minNum > maxNum:
        print("El primer parámentro debe de ser menor que el segundo parámentro.")

    def check_mirror(digit):
        switch = {
            "0": "0",
            "1": "1",
            "6": "9",
            "8": "8",
            "9": "6",
        }
        return switch.get(digit, False)

    def read_original(original, mirror):
        for i in range(len(original)):
            if check_mirror(original[i]) != mirror[i]:
                return False
        return True

    print('Los números reversibles dentro del rango indicado son: ')

    for i in range(minNum,maxNum+1):
        original = str(i)
        mirror = original[::-1]
        if read_original(original, mirror):
           # mirror = mirror.replace("6","a") podría volver a voltear el 6 y el 9, pero entonces el output de las dos columnas mostraría el mismo número.
           # mirror = mirror.replace("9","6") llegado a este punto es mejor revisar el código para ver como funciona y detecta los números "espejables"
           # mirror = mirror.replace("a","9") que fijarese en el propio output.
            print(original, mirror)