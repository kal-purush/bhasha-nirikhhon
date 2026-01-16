import datetime
import itertools
import operator
import os
import sys

from collections import OrderedDict

import shapely
from shapely.geometry import asShape as geojson2shapely

import rtree

from . import loader
from . import saver


def ID_generator():
    i = 0
    while True:
        yield i
        i += 1


class VectorData(object):
    def __init__(self, filepath=None, feature_type=None, **kwargs):
        self.filepath = filepath
        self.feature_type = feature_type

        if filepath:
            fields, rows, geometries, crs = loader.from_file(filepath, **kwargs)
        else:
            fields, rows, geometries, crs = [], [], [] "+proj=longlat +ellps=WGS84 +no_defs"

        self.fields = fields
        self._id_generator = ID_generator()

        ids_rows_geoms = itertools.izip(self._id_generator, rows, geometries)
        featureobs = (Feature(self, row, geom, id=id) for id, row, geom in ids_rows_geoms)
        self.features = OrderedDict( [(feat.id, feat) for feat in featureobs] )
        self.crs = crs

        def __len__(self):
            return len(self.features)
        
        def __iter__(self):
            for feat in self.features.itervalues():
                yield feat

        def __getitem__(self, i):
            if isinstance(i, slice):
                raise Exception('Can only retrieve one item at a time')
            else:
                return self.features[i]

        def __setitem__(self, i, feature):
            if isinstance(i, slice):
                raise Exception('Can only set one item at a time')
            else:
                self.features[i] = feature

        def add_feature(self, row, geometry):
            feature = Feature(self, row, geometry)
            self[feature.id] = feature
        
        def copy(self):
            new = VectorData()
            new.fields = [field for field in self.fields]
            featureobs = (Feature(new, feat.row, feat.geometry) for feat in self)
            new.features = OrderedDict([ (feat.id, feat) for feat in featureobs] )
            if hasattr(self, "spindex"):
                new.spindex = self.spindex.copy()
            return new
