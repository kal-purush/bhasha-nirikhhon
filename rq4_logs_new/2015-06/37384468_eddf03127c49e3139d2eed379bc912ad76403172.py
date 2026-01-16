# -*- coding: UTF-8 -*-
"""Python wrapper for stormlib
Credits goes to Christopher Chedeau for the initial python code version (linux)
https://github.com/vjeux/pyStormLib
"""

import ctypes
import os

import pystormlib.structure
import pystormlib.winerror

from pystormlib.utils import raise_for_error


try:
    dllpath = os.path.join(os.path.dirname(__file__), 'ressources\\stormlib_x86.dll')
    _stormlib = ctypes.WinDLL(dllpath)
except:
    raise RuntimeError("PyStormLib: can't locate stormlib_x86.dll")


class PyStormLib(object):
    def __init__(self, filepath=None):
        """PyStormLib wrapper for stormlib.

        :param filepath: A filepath to an MPQ file.
        :type filepath: str
        :raise: IOError if filename does not exists
        :raise: PyStormException in case something when wrong with stormlib
        """
        self.handle = ctypes.c_int()
        self.filepath = filepath
        if self.filepath:
            self.open_mpq_archive(self.filepath)

    def open_mpq_archive(self, filepath):
        """Opens a MPQ archive.

        :param filepath: A filepath to an MPQ file.
        :type filepath: str
        :raise: IOError if filename does not exists
        :raise: PyStormException in case something when wrong with stormlib
        """
        self.filepath = filepath
        if isinstance(filepath, str):
            filepath = filepath.encode('ascii')
        if not os.path.exists(filepath):
            raise IOError('{} not found'.format(filepath))

        raise_for_error(
            _stormlib.SFileOpenArchive, filepath, 0, 0, ctypes.byref(self.handle)
        )

    def search(self, pattern=None):
        """Search for a glob expression within an open MPQ Archive.

        ..code-block:: python

            adt_files = pystorm.search('*.adt')
            dbc_files = pystorm.search('*.dbc')

        :param pattern: a string representing a filename to search
        :type pattern: str
        :return: yields MPQFileData
        :rtype: generator
        :raise: PyStormException in case something when wrong with stormlib
        """
        if not pattern:
            pattern = '*'
        pattern = pattern.encode('ascii')

        file = pystormlib.structure.MPQFileData()
        result = raise_for_error(
            _stormlib.SFileFindFirstFile, self.handle, pattern, ctypes.byref(file), None
        )
        yield file
        file = pystormlib.structure.MPQFileData()
        try:
            while raise_for_error(_stormlib.SFileFindNextFile, result, ctypes.byref(file)):
                yield file
        except pystormlib.winerror.ErrorNoMoreFiles:
            pass

    def extract(self, filepath, destination):
        """Extract a file from an MPQ archive.
        If the destination directory doest not exists, the function will handle its creation by itself.

        ..code-block:: python

            pystorm.extract('World\\Maps\\Shadowfang\\Shadowfang_25_33.adt', "C:\\test\\")

        :param filepath: a string representing a filepath within an MPQ File
        :type filepath: str
        :param destination: a string representing a local directory to extract file.
        :type destination: str
        :return: yields MPQFileData
        :rtype: generator
        :raise: PyStormException in case something when wrong with stormlib
        """
        if isinstance(filepath, str):
            filepath = filepath.encode('ascii')
        if isinstance(destination, str):
            destination = destination.encode('ascii')
        destination = destination.replace(b'\\', b'/')
        try: os.makedirs(os.path.dirname(destination))
        except OSError:
            pass

        try: raise_for_error(_stormlib.SFileExtractFile, self.handle, filepath, destination, 0)
        except pystormlib.winerror.ErrorHandleEOF:
            pass

    def contains(self, filepath):
        """Search for a MPQ archive filename within the current opened MQP archive

        ..code-block:: python

            pystorm.contains('World\\Maps\\Shadowfang\\Shadowfang_25_33.adt')

        :param filepath: a string representing a filepath within an MPQ File
        :type filepath: str
        :return: True if filepath exsits
        :rtype: boolean
        :raise: PyStormException in case something when wrong with stormlib
        """
        if isinstance(filepath, str):
            filepath = filepath.encode('ascii')
        try: raise_for_error(_stormlib.SFileHasFile, self.handle, filepath)
        except pystormlib.winerror.PyStormException:
            return False
        return True

    def read(self, filepath):
        """Reads and return the content of a file from an MPQ archive.

        ..code-block:: python

            Shadowfang_25_33 = pystorm.read('World\\Maps\\Shadowfang\\Shadowfang_25_33.adt')

        :param filepath: a string representing a filepath within an MPQ File
        :type filepath: str
        :return: True if filepath exsits
        :rtype: boolean
        :raise: PyStormException in case something when wrong with stormlib
        """
        # Open the file
        file = ctypes.c_int()
        raise_for_error(_stormlib.SFileOpenFileEx, self.handle, filepath, 0, ctypes.byref(file))

        # Get the Size
        high = ctypes.c_int()
        low = raise_for_error(_stormlib.SFileGetFileSize, file, ctypes.byref(high))
        size = high.value * pow(2, 32) + low

        # Read the File
        data = ctypes.c_buffer(size)
        read = ctypes.c_int()
        raise_for_error(_stormlib.SFileReadFile, file, data, size, ctypes.byref(read), None)

        # Close and Return
        raise_for_error(_stormlib.SFileCloseFile, file)
        content = data.raw
        return content