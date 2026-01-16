"""Data Information."""

from collections.abc import Iterator

import ucdp as u

from .transtype import Data, TransType, get_transtype


class DataInfo(u.Object):
    """
    Data Information.

    Single Data:

        >>> datainfo = DataInfo.create(5)
        >>> datainfo
        DataInfo(transtype=SINGLE, data=5)
        >>> tuple(datainfo.iter())
        ((0, 5),)
        >>> tuple(datainfo.iter(baseaddr=0x1000, wordsize=8))
        ((4096, 5),)

    Burst Data:

        >>> datainfo = DataInfo.create((5, 6, 7))
        >>> datainfo
        DataInfo(transtype=BURST, data=(5, 6, 7))
        >>> tuple(datainfo.iter())
        ((0, 5), (4, 6), (8, 7))
        >>> tuple(datainfo.iter(baseaddr=0x1000, wordsize=8))
        ((4096, 5), (4104, 6), (4112, 7))

    Scattered Data:

        >>> datainfo = DataInfo.create(((16, 5), (28, 6), (60, 7)))
        >>> datainfo
        DataInfo(transtype=SCAT, data=((16, 5), (28, 6), (60, 7)))
        >>> tuple(datainfo.iter())
        ((16, 5), (28, 6), (60, 7))
        >>> tuple(datainfo.iter(baseaddr=0x1000, wordsize=8))
        ((4112, 5), (4124, 6), (4156, 7))

    Errors:

        >>> DataInfo.create('a')
        Traceback (most recent call last):
          ...
        TypeError: a
        >>> DataInfo.create(('a', 'b'))
        Traceback (most recent call last):
          ...
        TypeError: ('a', 'b')
    """

    transtype: TransType
    data: Data

    @staticmethod
    def create(data: Data, noscat: bool = False) -> "DataInfo":
        """Create :any:`DataInfo` for `data`."""
        transtype = get_transtype(data)
        return DataInfo(transtype=transtype, data=data)

    def iter(self, baseaddr: int = 0, wordsize: int = 4) -> Iterator[tuple[int, int]]:
        """Iteratate over single address value pairs according to access."""
        transtype = self.transtype
        if transtype == TransType.SINGLE:
            yield baseaddr, self.data
        elif transtype == TransType.BURST:
            for idx, value in enumerate(self.data):  # type: ignore[arg-type]
                yield (baseaddr + idx * wordsize), value
        else:
            for addr, value in self.data:  # type: ignore[union-attr]
                yield baseaddr + addr, value