class Colour:

    _MAX = 16**3 - 1

    def __init__(self, r: int, g: int, b: int, scl=1):
        self.r, self.g, self.b = r, g, b
        self._scl = self._MAX / scl

    def fmt(self) -> str:
        """#rrrgggbbb"""
        return f"#{self._fmt(self.r)}{self._fmt(self.g)}{self._fmt(self.b)}"

    def _fmt(self, val) -> str:
        return format(int(min(max(val * self._scl, 0), self._MAX)), "03x")

    def __eq__(self, other) -> bool:
        if self._scl == other._scl:
            return (self.r, self.g, self.b) == (other.r, other.g, other.b)
        return self.fmt() == other.fmt()