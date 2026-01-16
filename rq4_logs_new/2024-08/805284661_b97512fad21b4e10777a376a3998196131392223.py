"""Helper functions for the protein representation database"""

# Standard
from typing import List, Dict
from collections import defaultdict
from dataclasses import dataclass

# Third-party
import numpy as np
import pandas as pd

# Local
from moleculib.protein.datum import ProteinDatum


@dataclass
class Representation:
    """Representation object for a protein (sub)-structure. Corresponds to a row in the database."""
    pdb_id: str
    level: int
    level_idx: int
    scalar_rep: np.ndarray
    datum: ProteinDatum


class Representations:
    """Database of representation objects
    Distinguishes across different hierarchy levels
    """

    def __init__(
        self,
    ):

        # This is the primary key (i.e. should not be manually adjusted)
        self.ids: List[int] = []
        self.pdb_ids: List[str] = []
        self.levels: List[int] = []  # hierarchy level
        self.level_idxs: List[int] = []  # id within a hierarchy level
        self.scalars: List[np.ndarray] = []  # scalar representation
        self.datums: List[ProteinDatum] = []  # protein datum

        self._pk_counter: int = 0

    def add_representation(self, representation: Representation):
        """Add a `row` to the database"""
        self.ids.append(self._new_pk())
        self.pdb_ids.append(representation.pdb_id)
        self.levels.append(representation.level)
        self.level_idxs.append(representation.level_idx)
        self.scalars.append(representation.scalar_rep)
        self.datums.append(representation.datum)

    def _new_pk(self):
        self._pk_counter += 1  # increment the primary key counter
        return self._pk_counter

    def __repr__(self):
        return f"Representations object with {len(self.ids)} representations"

    def __len__(self):
        assert (
            len(self.ids)
            == len(self.pdb_ids)
            == len(self.levels)
            == len(self.level_idxs)
            == len(self.scalars)
            == len(self.datums)
        )
        return len(self.ids)


def populate_representations(
    encoded_dataset: Dict[str, Dict[int, np.ndarray]], sliced_dataset
):
    """Populate the representations object with the encoded dataset"""
    reps = Representations()
    mismatches = defaultdict(dict)
    for idcode in encoded_dataset:
        levels = encoded_dataset[idcode]
        for level in levels:
            embeddings = encoded_dataset[idcode][level]
            protein_data = sliced_dataset[idcode][level]
            n_embeddings, n_protein_datums = embeddings.shape[0], len(protein_data)

            if n_embeddings != n_protein_datums:
                difference = n_embeddings - n_protein_datums
                mismatches[idcode][level] = (n_embeddings, n_protein_datums)
                protein_data.extend([None] * difference)

            for level_idx in range(n_embeddings):
                scalar_representation = embeddings[level_idx]
                protein_datum = protein_data[level_idx]
                reps.add_representation(
                    Representation(
                        pdb_id=idcode,
                        level=level,
                        level_idx=level_idx,
                        scalar_rep=scalar_representation,
                        datum=protein_datum,
                    )
                )

    # Make it a DataFrame
    df = pd.DataFrame(
        {
            "pk": reps.ids,
            "PDBid": reps.pdb_ids,
            "levels": reps.levels,
            "level_idxs": reps.level_idxs,
            "scalars": reps.scalars,
            "datum": reps.datums,
        }
    )

    return reps, df, mismatches




def get_column(df, pk=None, pdb_id=None, level=None, level_idx=None, column=None):
    """Get a column in a DataFrame given certain conditions.
    If no column is specified, return a view of the indexed
    DataFrame.
    """

    # Build out the indexing condition manually
    condition = True
    if pk is not None:
        if not isinstance(pk, list):
            pk = [pk]
        condition = condition & (df["pk"].isin(pk))
    if pdb_id is not None:
        if not isinstance(pdb_id, list):
            pdb_id = [pdb_id]
        condition = condition & (df["PDBid"].isin(pdb_id))
    if level is not None:
        if not isinstance(level, list):
            level = [level]
        condition = condition & (df["level"].isin(level))
    if level_idx is not None:
        if not isinstance(level_idx, list):
            level_idx = [level_idx]
        condition = condition & (df["level_idx"].isin(level_idx))

    if condition is True:
        condition = df.index  # return all rows
    if column is None:
        return df.loc[condition]
    return df.loc[condition, column].values


def get_scalars(df, pk=None, pdb_id=None, level=None, level_idx=None):
    """Return the scalars given a selection. If `level_idx` is None
    then return the whole column (all the representation vectors
    for a specific PDBid given a hierarchy level)
    """
    return get_column(df, pk, pdb_id, level, level_idx, "scalars")