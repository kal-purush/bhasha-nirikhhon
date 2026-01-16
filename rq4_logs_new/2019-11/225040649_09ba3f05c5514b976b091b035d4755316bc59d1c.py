from typing import List


__all__ = ["Args"]


class Args:

    def __init__(self, pattern: str, values: List[str]):
        self.pattern = pattern
        self.values = values

    def get_bool(self, key: str) -> bool:
        return False

    def get_int(self, key: str) -> int:
        return 0

    def get_string(self, key: str) -> str:
        return 'ouch!'