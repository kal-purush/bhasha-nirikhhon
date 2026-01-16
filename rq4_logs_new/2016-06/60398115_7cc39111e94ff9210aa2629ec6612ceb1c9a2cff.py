#!/usr/bin/env python2

from __future__ import with_statement
import os
import sys
from errno import *
import time
import stat
import xml.etree.ElementTree as ElementTree
from fuse import FUSE, FuseOSError, Operations

class XmlToDict():
    def __init__(self, conf):
        tree = ElementTree.parse(conf)
        root = tree.getroot()

        # Apply the real path prefix (if any)
        self.realpath_prefix = "" if 'basepath' not in dict(root.items()) else dict(root.items())['basepath']
        
        # Process the XML file
        self.path_contents = {}
        self.fake_to_real = {}
        self.checkTag(root, '/')

    def checkTag(self, tag, path):
        for element in tag:
          # We record the contents for the path
          if path not in self.path_contents:
              self.path_contents[path] = []
          self.path_contents[path].append(dict(element.items())['name'])

          objpath = ('' if path == '/' else path) + '/' + dict(element.items())['name']
          if len(element): # We are a folder with children, we recurse
              self.checkTag(element, objpath)
          elif element.tag == 'File': # File, we record the mapping from full fakepath to full realpath (including global prefix)
              self.fake_to_real[objpath] = self.realpath_prefix + dict(element.items())['realpath']
          elif element.tag == 'Dir': # Empty folder
              if objpath not in self.path_contents:
                  self.path_contents[objpath] = []

class FuseXml(Operations):
    def __init__(self, xml):
        # We parse the XML configuration file
        # * We need a list of contents for each path: self.config.path_contents
        #   The keys also act as a recap of all folders
        # * We need the realpath for each file: self.config.fake_to_real
        #   The keys also act as a recap of all files
        self.config = XmlToDict(xml)

        self.atime = int(time.time())
        self.uid = 1000
        self.gid = 1000

    # Filesystem methods
    # ==================

    def getattr(self, path, fh=None):
        fk = {'st_atime': self.atime, 'st_ctime': self.atime, 'st_gid': self.gid, 'st_mode': 0, 'st_mtime': self.atime, 'st_nlink': 2, 'st_size': 0, 'st_uid': self.uid}
        if path in self.config.path_contents: # It's a folder
            fk['st_mode'] = stat.S_IFDIR | 0555
            return fk
        elif path in self.config.fake_to_real: # It's a file
            fk['st_mode'] = stat.S_IFREG | 0444
            st = os.lstat(self.config.fake_to_real[path])
            fk['st_size'] = getattr(st, 'st_size')
            return fk
        else:
            raise FuseOSError(EROFS)

    def readdir(self, path, fh):
        if path not in self.config.path_contents:
            raise FuseOSError(EROFS)
        yield '.'
        yield '..'
        for entry in self.config.path_contents[path]:
            yield entry

    # File methods
    # ============

    def open(self, path, flags):
        if path not in self.config.fake_to_real:
            raise FuseOSError(EROFS)
        full_path = self.config.fake_to_real[path]
        return os.open(full_path, flags)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def release(self, path, fh):
        return os.close(fh)


def main(xml, mountpoint):
    FUSE(FuseXml(xml), mountpoint, nothreads=True, foreground=True)

if __name__ == '__main__':
    main(sys.argv[1], sys.argv[2])