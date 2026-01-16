"""
An abstract machine. Contains a number of runtimes and transition logic
"""


class _Machine(object):
    """
    """
    def __init__(self, name: str):
        self._name = name

        self._runtimes = []
        self._transitions = []


    def to_code(self) -> str:
        """
        Convert this machine into C++ ready for compiliation
        """
        return ""