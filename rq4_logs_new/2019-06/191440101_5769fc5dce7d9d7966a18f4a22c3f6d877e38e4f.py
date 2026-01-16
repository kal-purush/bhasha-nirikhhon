class MyIterator:

    def __init__(self):
        print("Je m'initialise à 40")
        self.i = 40

    def __iter__(self):
        print("on a appelé iter")
        return self

    def __next__(self):
        print("On a appelé next")
        self.i += 2
        if self.i > 56:
            raise StopIteration()
        return self.i


def my_generator():
    i = 40
    while i < 56:
        i += 2
        yield i


def main():
    iterator = MyIterator()
    for i in iterator:
        print(i)

    for i in my_generator():
        print(i)


if __name__ == "__main__":
    main()