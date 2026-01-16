from pathlib import Path

from .utils import get_args_from_filename, load_csv, save_csv, get_coords
from .geoindex import GeoIndex
from .distance import distance

class Table:

    def __init__(self, filename, verbose=False):

        self.verbose = verbose

        # Parse filename (extract optional arguments, check that the file exists)
        fn, self.args = get_args_from_filename(filename)
        fn = Path(fn)
        assert fn.exists(), fn
        self.filename = Path(fn)

        self.header, self.data = load_csv(self.filename)
        if verbose:
            print(' - Filename:', str(self.filename))
            print(' - Headers:', self.header)
            print(' - Number of rows:', len(self.data))

        self.lat_i = self.header.index(self.args['lat'])
        self.lon_i = self.header.index(self.args['lon'])
        self.id_i = self.header.index(self.args['id'])

        self._index = None

    @property
    def index(self):
        # Create index if needed
        if self._index is None:
            coords = get_coords(self.header, self.data, self.args)
            self._index = GeoIndex(data=coords)
        return self._index


    def add_distance_to_point(self, lat, lon, name):
        assert name not in self.header
        self.header.append(name)

        verbose = self.verbose
        lat_i = self.lat_i
        lon_i = self.lon_i

        for i, row in enumerate(self.data, 1):
            if verbose and (i % 1000 == 0):
                print('.', end='', flush=True)
            row_lat, row_lon = float(row[lat_i]), float(row[lon_i])
            dist = distance(row_lat, row_lon, lat, lon)
            row.append(f'{dist:.4f}')
        if verbose and i >= 1000:
            print()


    def add_distance_to_table(self, other_table, id_name, dist_name):

        if self.verbose:
            print(f' - Computing distance to nearest neighbor from {other_table.filename}; storing distance in "{dist_name}" and neighbor id in "{id_name}"')
        
        assert id_name not in self.header
        assert dist_name not in self.header
        self.header.extend([id_name, dist_name])

        # Create index
        idx = other_table.index

        verbose = self.verbose
        lat_i = self.lat_i
        lon_i = self.lon_i

        # Compute all distances
        for i, row in enumerate(self.data, 1):
            if verbose and (i % 1000 == 0):
                print('.', end='', flush=True)
            
            lat = float(row[lat_i])
            lon = float(row[lon_i])
            
            neighbor_id = idx.nearest(lat, lon)
            dist = distance(lat, lon, *idx.coords[neighbor_id])
            row.extend([f'{dist:.4f}', neighbor_id])

        if verbose and i >= 1000:
            print()


    def add_distance_to_self(self, id_name, dist_name):

        if self.verbose:
            print(f' - Computing distance to nearest neighbor from self; storing distance in "{dist_name}" and neighbor id in "{id_name}"')

        assert id_name not in self.header
        assert dist_name not in self.header
        self.header.extend([id_name, dist_name])

        # Create index
        idx = self.index

        verbose = self.verbose
        lat_i = self.lat_i
        lon_i = self.lon_i
        id_i = self.id_i

        # Compute all distances
        for i, row in enumerate(self.data, 1):
            if verbose and (i % 1000 == 0):
                print('.', end='', flush=True)
            
            lat = float(row[lat_i])
            lon = float(row[lon_i])
            identifier = int(row[id_i])

            neighbor_id = idx.nearest(lat, lon, identifier=identifier)
            dist = distance(lat, lon, *idx.coords[neighbor_id])
            row.extend([f'{dist:.4f}', neighbor_id])

        if verbose and i >= 1000:
            print()


    def save(self, filename):
        if self.verbose:
            print(f' - Saving output in {filename}')
        delimiter = ',' if filename.suffix == '.csv' else '\t'
        save_csv(filename, self.header, self.data, delimiter=delimiter)