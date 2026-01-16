"""Module, Word and Field Reference."""

from typing import TypeAlias

import ucdp as u

from .addrspace import Addrspace, Field, Word


class AddrMapRef(u.Object):
    """Address Map Reference."""

    addrspace: Addrspace
    """Address Space."""

    word: Word | None = None
    """Word."""

    field: Field | None = None
    """Field."""

    def __str__(self) -> str:
        result = f"{self.addrspace.name}"
        if self.word:
            result = f"{result}.{self.word.name}"
            if self.field:
                result = f"{result}.{self.field.name}"
        return result


RawAddrMapRef: TypeAlias = AddrMapRef | Addrspace | str
"""Unresolved Address Map Reference."""