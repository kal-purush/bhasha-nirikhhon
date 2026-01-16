class __MetaBitField(type):
    def __new__(cls, name, bases, attrs):
        field = 0
        pos = 0
        for attr, val in attrs.items():
            if isinstance(val, bool):
                attrs[attr] = cls.__field(pos, attr)
                if val:
                    field |= (1 << pos)
                pos += 1
        attrs["_bit_field_int_"] = field
        return super().__new__(cls, name, bases, attrs)

    def __field(pos: int, attr: str):
        pos = 1 << pos
        @property
        def bit_field(self) -> bool:
            return bool(self._bit_field_int_ & pos)
        @bit_field.setter
        def bit_field(self, value):
            if value:
                self._bit_field_int_ |= pos
            else:
                self._bit_field_int_ &= ~pos
        return bit_field

    def __repr__(self) -> str:
        return f"{self.__name__}<{bin(self._bit_field_int_)[2:]}>"
    def __str__(self) -> str:
        return bin(self._bit_field_int_)[2:]

class BitField(metaclass=__MetaBitField):
    def __init__(self):
        pass
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}<{bin(self._bit_field_int_)[2:]}>"

    def __str__(self) -> str:
        return bin(self._bit_field_int_)[2:]
    def format(self, size: int=None) -> str:
        if size is None:
            return str(self)
        return format(self._bit_field_int_, f"0{size}b")

    def bit_parse(self, field: int):
        if isinstance(field, self.__class__):
            self._bit_field_int_ = field._bit_field_int_
        else:
            self._bit_field_int_ = field
    def value(self) -> int:
        return self._bit_field_int_