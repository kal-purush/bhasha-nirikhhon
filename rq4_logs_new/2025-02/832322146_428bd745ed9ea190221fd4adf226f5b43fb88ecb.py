"""
Address Information.

Access can be done to addrspacesa, words and fields,
and addresses in a single, burst or scatter flavour using offsets, masks, etc.
:any:`create` takes all these variants into account and serves a normalized information set `AddrInfo`.
"""

from collections.abc import Iterator
from typing import TypeAlias

import ucdp as u
from pydantic import PositiveInt

from .addrmap import AddrMap
from .addrmapref import AddrMapRef, RawAddrMapRef
from .addrrange import AddrRange
from .resolver import resolve

Size: TypeAlias = PositiveInt
Depth: TypeAlias = PositiveInt


class AddrInfo(u.Object):
    """Address Info."""

    ref: AddrMapRef
    addrrange: AddrRange
    mask: u.Hex | None = None

    @staticmethod
    def create(addrmap: AddrMap, item: RawAddrMapRef) -> "AddrInfo":
        """
        Create `AddrInfo`.

        Args:
            addrmap: Address Map
            item: Thing to be resolved

        Example:
            >>> import ucdp as u
            >>> from ucdp_addr import Addrspace, Word, Field, AddrMap
            >>> addrmap = AddrMap()
            >>> for idx, aname in enumerate(("uart", "spi", "owi")):
            ...     addrspace = Addrspace(name=aname, width=32, depth=8, baseaddr=8*4*idx)
            ...     addrmap.add(addrspace)
            ...     word = addrspace.add_word("ctrl")
            ...     field = word.add_field("ena", u.BitType(), "RW")
            ...     word = addrspace.add_word("stat")
            ...     field = word.add_field("bsy", u.BitType(), "RO", offset=9)

        Addrspace:

            >>> addrinfo = AddrInfo.create(addrmap, "spi")
            >>> addrinfo.ref
            AddrMapRef(addrspace=Addrspace(name='spi', baseaddr=Hex('0x20'), size=Bytesize('32 bytes')))
            >>> addrinfo.addrrange.info
            '0x20 8x32 (32 bytes)'
            >>> addrinfo.mask
            >>> tuple(addrinfo.iter())
            (Hex('0x20'), Hex('0x24'), Hex('0x28'), Hex('0x2C'), Hex('0x30'), Hex('0x34'), Hex('0x38'), Hex('0x3C'))

        Word:

            >>> addrinfo = AddrInfo.create(addrmap, "spi.stat")
            >>> addrinfo.ref
            AddrMapRef(addrspace=Addrspace(name='spi', baseaddr=Hex('0x20'), ..., word=Word(name='stat', ...)
            >>> addrinfo.addrrange.info
            '0x24 1x32 (4 bytes)'
            >>> addrinfo.mask
            >>> tuple(addrinfo.iter())
            (Hex('0x24'),)

        Field:

            >>> addrinfo = AddrInfo.create(addrmap, "spi.stat.bsy")
            >>> addrinfo.ref
            AddrMapRef(...Hex('0x20'), ... word=Word(name='stat', ...(name='bsy', type_=BitType(), bus=RO, offset=9))
            >>> addrinfo.addrrange.info
            '0x24 1x32 (4 bytes)'
            >>> addrinfo.mask
            Hex('0x200')
            >>> tuple(addrinfo.iter())
            (Hex('0x24'),)

        """
        ref = resolve(addrmap, item)
        mask = None
        if ref.field:
            if mask is not None:
                raise ValueError("'mask' is not allowed for fields")
            return AddrInfo(ref=ref, addrrange=ref.addrrange, mask=ref.field.slice.mask)

        if ref.word:
            addrrange = ref.addrrange
            return AddrInfo(ref=ref, addrrange=addrrange, mask=mask)

        return AddrInfo(ref=ref, addrrange=ref.addrrange, mask=mask)

    def iter(self) -> Iterator[u.Hex]:
        """Iteratate over address ranges."""
        addrrange = self.addrrange
        wordsize = addrrange.wordsize
        for idx in range(addrrange.depth):
            yield addrrange.baseaddr + int(idx * wordsize)