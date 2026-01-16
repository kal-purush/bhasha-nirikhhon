"""File readers."""

import csv
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

import numpy as np
from nexusformat.nexus import nxload
from orsopy.fileio import load_orso
from RATapi.models import Data


class AbstractDataReader(ABC):
    """Abstract base class for reading a data file."""

    @abstractmethod
    def read(self, filepath: str | Path) -> Iterable[Data]:
        """Read data to a list of Data objects.

        Parameters
        ----------
        filepath: str or Path
            The path to the data file.

        Returns
        -------
        Iterable[Data]
            An iterable of all data objects in the file.

        """
        raise NotImplementedError


class TextDataReader(AbstractDataReader):
    """Reader for plain text data files."""

    def read(self, filepath: str | Path) -> Iterable[Data]:
        with open(filepath) as datafile:
            sniffer = csv.Sniffer()
            has_header = sniffer.has_header(datafile.read(1024))
            datafile.seek(0)
            delimiter = sniffer.sniff(datafile.read(1024)).delimiter
            datafile.seek(0)

        data = np.loadtxt(filepath, delimiter=delimiter, skiprows=int(has_header))

        yield Data(name=Path(filepath).stem, data=data)


class AscDataReader(AbstractDataReader):
    """Reader for ISIS Histogram data files."""

    def read(self, filepath: str | Path) -> Iterable[Data]:
        data = np.loadtxt(filepath, delimiter=",")

        # data processing from rascal-1:
        # https://github.com/arwelHughes/RasCAL_2019/blob/master/Rascal_functions/hist2xy.m
        for i, row in enumerate(data[:-1]):
            if row[1] != 0:
                row[0] += (data[i + 1, 0] - row[0]) / 2

        yield Data(name=Path(filepath).stem, data=data)


class NexusDataReader(AbstractDataReader):
    """Reader for Nexus data files."""

    def read(self, filepath: str | Path) -> Iterable[Data]:
        for entry in nxload(filepath, "r").NXentry:
            if not entry.NXdata:
                raise ValueError("Nexus file does not seem to contain NXdata.")
            for data_group in entry.NXdata:
                q_values = np.array(data_group.plot_axes)
                # axes are bins so take centre of each bin
                q_values = (q_values[:, :-1] + q_values[:, 1:]) / 2
                signal = data_group.nxsignal.nxdata
                errors = data_group.nxerrors.nxdata

                data = np.vstack([q_values, signal, errors])
                data = data.transpose()

                yield Data(name=data_group.nxname, data=data)


class OrtDataReader(AbstractDataReader):
    """Reader for ORSO reflectivity data files."""

    def read(self, filepath: str | Path) -> Iterable[Data]:
        orso_data = load_orso(filepath)
        for dataset in orso_data:
            yield Data(name=dataset.info.data_source.sample.name, data=dataset.data)


readers = defaultdict(
    lambda: TextDataReader,
    {
        ".asc": AscDataReader,
        ".nxs": NexusDataReader,
        ".ort": OrtDataReader,
    },
)