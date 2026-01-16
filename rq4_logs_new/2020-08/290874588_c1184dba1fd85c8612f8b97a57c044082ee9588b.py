# -*- coding: utf-8 -*-
"""
    binalyzer_wasm
    ~~~~~~~~~~~~~~

    Binalyzer WebAssembly Extension

    :copyright: 2020 Denis Vasilík
    :license: MIT, see LICENSE for details.
"""

name = "binalyzer_wasm"

__tag__ = ""
__build__ = 0
__version__ = "{}".format(__tag__)
__commit__ = "0000000"

from .extension import WebAssemblyExtension
from .wasm import (
    LEB128UnsignedValueConverter,
    LEB128UnsignedBindingValueProvider,
    LEB128SizeBindingValueProvider,
)