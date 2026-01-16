class Triangle:
    def __init__(self, first, second):
        if not isinstance(first, (int, float)) or first <= 0:
            raise ValueError("Катет 'a' должен быть положительным числом.")
        if not isinstance(second, (int, float)) or second <= 0:
            raise ValueError("Катет 'b' должен быть положительным числом.")
        self.first = first
        self.second = second

    def read(self):
        while True:
            try:
                self.first = float(input("Введите катет 'a': "))
                if self.first <= 0:
                    raise ValueError("Катет 'a' должен быть положительным числом.")
                break
            except ValueError as e:
                print(e)
        while True:
            try:
                self.second = float(input("Введите катет 'b': "))
                if self.second <= 0:
                    raise ValueError("Катет 'b' должен быть положительным числом.")
                break
            except ValueError as e:
                print(e)

    def display(self):
        print(f"Катет 'a': {self.first}")
        print(f"Катет 'b': {self.second}")

    def hypotenuse(self):
        return (self.first**2 + self.second**2)**0.5

def make_Triangle(first, second):
    try:
        return Triangle(first, second)
    except ValueError as e:
        print(e)
        return None

if __name__ == "__main__":

    triangle2 = Triangle(1, 1)
    triangle2.read()

    print(f"Гипотенуза: {triangle2.hypotenuse()}")

    """triangle1 = Triangle(3, 4)


    triangle1.display()

    hypotenuse = triangle1.hypotenuse()
    print(f"Гипотенуза: {hypotenuse}")"""

    """triangle3 = make_Triangle(5, 12)
    if triangle3:
        triangle3.display()
        print(f"Гипотенуза: {triangle3.hypotenuse()}")"""