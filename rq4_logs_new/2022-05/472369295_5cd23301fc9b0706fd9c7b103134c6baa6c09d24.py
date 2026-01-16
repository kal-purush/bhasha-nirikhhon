from pathlib import Path
from typing import Union

PathLike = Union[Path, str, bytes]


class NyaCommand:
    DECREMENT = "n"
    INCREMENT = "y"
    OUTPUT = "a"
    RESET = "~"


EMPTY = str()


def print_char(value: int) -> None:
    """Interprets the `value` as an Unicode codepoint and prints it."""
    print(chr(value), end=EMPTY)


class Nya:
    """The `nya~` interpreter.

    - `n` decrements the counter;
    - `y` increments the counter;
    - `a` interprets the counter as an Unicode codepoint and prints it;
    - `~` resets the counter back to 0.
    """

    def __init__(self) -> None:
        self._state = 0

    def execute(self, source: str) -> None:
        """Executes the `source` string."""
        for char in source:
            match char:
                case NyaCommand.DECREMENT:
                    self._state -= 1

                case NyaCommand.INCREMENT:
                    self._state += 1

                case NyaCommand.OUTPUT:
                    print_char(self._state)

                case NyaCommand.RESET:
                    self._state = 0

                case _:
                    continue

        print()

    def execute_path(self, path: PathLike) -> None:
        """Fetches the code from `path` and executes it."""
        with open(path) as file:
            self.execute(file.read())