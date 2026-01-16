class Type:
    """Layer Type is a collection of identifiers for Layer Type Items"""

    class Item:
        """The individual Items that are used as a label to group elements in the Layer Stack"""
        def __init__(self, name: str, value: int):
            """Should not be created by the user
            name: The name used to access through the type
            value: The weight used to sort the Stack
            """
            self.name = name
            self.value = value

        def __repr__(self) -> str:
            return f"{self.name}.{self.value}"

        def __lt__(self, other: "Item") -> bool:
            """Sorts based on the value"""
            return self.value < other.value

    def __init__(self, name: str, **objs: int):
        """Collection of Items that is effectivly a non-constant enum.
        objs is the key value pairs used to generate the items.
        Items are accessed as memeber variables of this object.
        """
        self._name = name
        self.__items = {"NONE": self.__class__.Item("NONE", 0), **{k.upper(): self.__class__.Item(k.upper(), v) for k,v in objs.items() if not k.startswith("_")}}
        self.__ord_items = sorted(self.__items.values())

    def __getitem__(self, key: str) -> Item:
        return self.__items[key.upper()]

    def __iter__(self):
        return self.__ord_items.__iter__()

    def __setitem__(self, key: str, value: int) -> Item:
        key = key.upper()
        if key in self.__items:
            item = self.__items[key]
            item.value = value
        else:
            item = self.__class__.Item(key, value)
            self.__items[key] = item
            self.__ord_items.append(item)
            self.__ord_items.sort()
        return item

    def __delitem__(self, key: str) -> Item:
        item = self.__items.pop(key.upper())
        try:
            item.value = None
            self.__ord_items.remove(item)
        except AttributeError:    pass
        self.__ord_items.sort()
        return item

    def __getattribute__(self, key: str) -> Item:
        if key.startswith("_"):
            return super().__getattribute__(key)
        return self.__items[key.upper()]

    def __repr__(self) -> str:
        return f"{self._name}"


r = Type("Render", top=2, bottom=3)