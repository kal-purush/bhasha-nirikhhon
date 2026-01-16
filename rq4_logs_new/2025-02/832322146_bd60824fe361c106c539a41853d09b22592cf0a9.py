#
# MIT License
#
# Copyright (c) 2024 nbiotcloud
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""
Access.
"""

from collections.abc import Sequence
from typing import Annotated, Literal

import ucdp as u
from pydantic import (
    BeforeValidator,
    PlainSerializer,
    WithJsonSchema,
)


class ReadOp(u.IdentLightObject):
    """
    Read Operation.

    NEXT = {data}DATA
    """

    data: Literal[None, 0, 1, "~"] = None
    """Operation On Stored Data."""
    once: bool = False
    """Operation is just allowed once."""
    title: str = u.Field(repr=False)
    """Title."""
    descr: str = u.Field(repr=False)
    """Description."""


_R = ReadOp(name="R", title="Read", descr="Read without Modification.")
_RC = ReadOp(name="RC", data=0, title="Read-Clear", descr="Clear on Read.")
_RS = ReadOp(name="RS", data=1, title="Read-Set", descr="Set on Read.")
_RT = ReadOp(name="RT", data="~", title="Read-Toggle", descr="Toggle on Read.")
_RP = ReadOp(name="RP", once=True, title="Read-Protected", descr="Data is hidden after first Read.")


class WriteOp(u.IdentLightObject):
    """
    Write Operation.

    NEXT = {data}DATA {op} {write}WRITE
    """

    data: Literal[None, "", "~"] = None
    """Operation On Stored Data."""
    op: Literal[None, 0, 1, "&", "|"] = None
    """Operation On Stored and Incoming Data."""
    write: Literal[None, "", "~"] = None
    """Operation On Incoming Data."""
    once: bool = False
    """Operation is just allowed once."""
    title: str = u.Field(repr=False)
    """Title."""
    descr: str = u.Field(repr=False)
    """Description."""


_W = WriteOp(name="W", write="", title="Write", descr="Write Data.")
_W0C = WriteOp(name="W0C", data="", op="&", write="", title="Write-Zero-Clear", descr="Clear On Write Zero.")
_W0S = WriteOp(name="W0S", data="", op="|", write="~", title="Write-Zero-Set", descr="Set On Write Zero.")
_W1C = WriteOp(name="W1C", data="", op="&", write="~", title="Write-One-Clear", descr="Clear on Write One.")
_W1S = WriteOp(name="W1S", data="", op="|", write="", title="Write-One-Set", descr="Set on Write One.")
_WL = WriteOp(name="WL", write="", once=True, title="Write Locked", descr="Write Data once and Lock.")


class Access(u.IdentLightObject):
    """Access."""

    read: ReadOp | None = None
    write: WriteOp | None = None

    @property
    def title(self):
        """Title."""
        readtitle = self.read and self.read.title
        writetitle = self.write and self.write.title
        if readtitle and writetitle:
            return f"{readtitle}/{writetitle}"
        return readtitle or writetitle

    @property
    def descr(self):
        """Description."""
        readdescr = self.read and self.read.descr
        writedescr = self.write and self.write.descr
        if readdescr and writedescr:
            return f"{readdescr} {writedescr}"
        return readdescr or writedescr

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.name


NA = Access(name="NA")

RO = Access(name="RO", read=_R)
RC = Access(name="RC", read=_RC)
RS = Access(name="RS", read=_RS)
RT = Access(name="RT", read=_RT)
RP = Access(name="RP", read=_RP)

WO = Access(name="WO", write=_W)
W0C = Access(name="W0C", write=_W0C)
W0S = Access(name="W0S", write=_W0S)
W1C = Access(name="W1C", write=_W1C)
W1S = Access(name="W1S", write=_W1S)
WL = Access(name="WL", write=_WL)

RW = Access(name="RW", read=_R, write=_W)
RW0C = Access(name="RW0C", read=_R, write=_W0C)
RW0S = Access(name="RW0S", read=_R, write=_W0S)
RW1C = Access(name="RW1C", read=_R, write=_W1C)
RW1S = Access(name="RW1S", read=_R, write=_W1S)
RWL = Access(name="RWL", read=_R, write=_WL)


ACCESSES = u.Namespace(
    (
        RO,
        RC,
        RS,
        RT,
        RP,
        WO,
        W0C,
        W0S,
        W1C,
        W1S,
        WL,
        RW,
        RW0C,
        RW0S,
        RW1C,
        RW1S,
        RWL,
    )
)
ACCESSES.lock()


def cast_access(value: str | Access) -> Access:
    """
    Cast Access.

    Usage:

        >>> from ucdp_addr import addrspace
        >>> access = addrspace.cast_access("RO")
        >>> access
        RO
        >>> cast_access(access)
        RO
    """
    if isinstance(value, Access):
        return value
    return ACCESSES[value]


ToAccess = Annotated[
    Access,
    BeforeValidator(lambda x: cast_access(x)),
    PlainSerializer(lambda x: str(x), return_type=str),
    WithJsonSchema({"type": "string"}, mode="serialization"),
]


_COUNTERACCESS = {
    None: RO,
    RO: RW,
    # RC: ,
    # RS: ,
    # RI: ,
    WO: RO,
    # W1C: ,
    # W1S: ,
    RW: RO,
    # RW1C: ,
    # RW1S: ,
    # RCW: ,
    # RCW1C: ,
    # RCW1S: ,
    # RSW: ,
    # RSW1C: ,
    # RSW1S: ,
    # RIW: ,
    # RIW1C: ,
    # RIW1S: ,
}


def get_counteraccess(access: Access) -> Access | None:
    """
    Get Counter Access.

    Usage:

        >>> from ucdp_addr import get_counteraccess, access
        >>> str(get_counteraccess(access.RO))
        'RW'
        >>> str(get_counteraccess(access.RW))
        'RO'
    """
    return _COUNTERACCESS.get(access, None)


def any_read(accesses: Sequence[Access]) -> bool:
    """
    Return `True` if there is any read.

    >>> any_read([NA])
    False
    >>> any_read([RO, RO])
    True
    >>> any_read([RW, RW])
    True
    >>> any_read([RO, RW, WO])
    True
    """
    return any(access.read for access in accesses)


def is_read_repeatable(accesses: Sequence[Access]) -> bool:
    """
    Return `True` if a read has no side-effects.

    >>> is_read_repeatable([NA])
    True
    >>> is_read_repeatable([RO, RO])
    True
    >>> is_read_repeatable([RW, RW])
    True
    >>> is_read_repeatable([RO, RW, WO])
    True
    >>> is_read_repeatable([RO, RW, RC])
    False
    >>> is_read_repeatable([RO, RW, RP])
    False
    """
    return all(not acc.read or not (acc.read.data is not None or acc.read.once) for acc in accesses)


def is_write_repeatable(accesses: Sequence[Access]) -> bool:
    """
    Return `True` if a write has no side-effects.

    >>> is_write_repeatable([NA])
    True
    >>> is_write_repeatable([RO, RO])
    True
    >>> is_write_repeatable([RW, RW])
    True
    >>> is_write_repeatable([RO, RW, WO])
    True
    >>> is_write_repeatable([RO, RW, WL])
    False
    """
    return all(not acc.write or not acc.write.once for acc in accesses)