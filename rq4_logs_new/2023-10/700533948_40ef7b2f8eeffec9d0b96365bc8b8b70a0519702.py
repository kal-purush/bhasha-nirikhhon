from items.equippable import Equippable

class Cloak(Equippable):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__("cloak", *args, **kwargs)