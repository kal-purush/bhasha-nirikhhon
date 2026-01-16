#! /usr/bin/env python2

from cffi import FFI
ffi = FFI ()

rust_component = ffi.dlopen ("target/debug/librust_component.so")