# example_class.py


class Duck:

    def __init__(self, name: str) -> None:
        self.name = name

    def says(self, text: str = "quack quack") -> str:
        return f"{self.name} says: {text}!"


def duck_annotations() -> None:
    duck = Duck("Mr. Duck")
    print(duck.says.__annotations__)


def more_annotations_stuff() -> None:
    my_var: str  # this line only creates the annotations
    # print(my_var)  # so this line will fail

    my_second_var: str = "Hello"  # create both annotation and var
    print(my_second_var)


def creating_alias() -> None:
    from typing import List

    def get_duck_list() -> List[Duck]:
        return [Duck(f"{idx}") for idx in range(5)]

    duck_list = get_duck_list()
    for duck in duck_list:
        print(duck.says("Hey there!"))


if __name__ == "__main__":
    duck_annotations()
    more_annotations_stuff()
    creating_alias()