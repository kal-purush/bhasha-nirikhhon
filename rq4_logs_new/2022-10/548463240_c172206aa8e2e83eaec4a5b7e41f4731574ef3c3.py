# Copyright (C) 2022, luca.baldini@pi.infn.it
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.


from dataclasses import dataclass
from enum import Enum, auto

from loguru import logger
import pandas as pd


FILE_PATH = 'lab1_20221009_edit.xlsx'

MACRO_GROUPS = ['A1', 'B1', 'A2', 'B2']

logger.info('Reading in group changes...')
GROUP_CHANGES = {}
df = pd.read_excel(FILE_PATH, sheet_name='Cambi')
for _, row in df.iterrows():
    GROUP_CHANGES[row['Matricola']] = row['Gruppo']
logger.info(f'Done: {GROUP_CHANGES}')


@dataclass
class Student:

    """Small class encapsulating a student.
    """

    name : str
    surname : str
    identifier : int
    email : str
    macro_group : str
    companion_name : str = None
    companion_surname : str = None
    notes : str = None

    def __post_init__(self) -> None:
        """Post initialization.
        """
        try:
            expected = GROUP_CHANGES[self.identifier]
        except KeyError:
            expected = MACRO_GROUPS[self.identifier % 4]
        if self.macro_group != expected:
            logger.error(f'Group for {self.full_name()} is {self.macro_group} instead of {expected}')

    def full_name(self) -> str:
        """Return the full name.
        """
        return f'{self.name} {self.surname}'

    def companion_full_name(self) -> str:
        """Return the full name of the companion (if available).
        """
        if self.companion_name is None and self.companion_surname is None:
            return None
        return f'{self.companion_name} {self.companion_surname}'



class DataBase(dict):

    """The glorious student database.

    ID
    Start time
    Completion time
    Email
    Name
    Nome
    Cognome
    Numero di matricola
    Macro-gruppo
    Nome del/della compagno/a di gruppo (opzionale)
    Cognome del/della compagno/a di gruppo (opzionale)
    Eventuali note o richieste specifiche (opzionale)
    """

    class Fields(Enum):

        """Nested enums for the column names.
        """

        Name = 'Nome'
        Surname = 'Cognome'
        Identifier = 'Numero di matricola'
        Email = 'Email'
        Group = 'Macro-gruppo'
        CompanionName = 'Nome compagno'
        CompanionSurname = 'Cognome compagno'
        Notes = 'Note'

    # Simple lambda functions for reading in the cell content.
    format_name = lambda text: text.title().strip()
    format_identifier = lambda text: int(float(text))

    # Converters for the columns.
    CONVERTERS = {
        Fields.Name.value : format_name,
        Fields.Surname.value : format_name,
        Fields.Identifier.value : format_identifier,
        Fields.CompanionName.value : format_name,
        Fields.CompanionSurname.value : format_name,
    }

    def __init__(self, file_path : str):
        """Constructor.
        """
        super().__init__(self)
        logger.info(f'Reading input data from {file_path}...')
        _df = pd.read_excel(file_path, converters=self.CONVERTERS)
        logger.info(f'Done, {len(_df)} row(s) found.')
        col_names = [field.value for field in self.Fields]
        for _, row in _df.iterrows():
            args = [row[col] if pd.notna(row[col]) else None for col in col_names]
            student = Student(*args)
            if row['Name'] != student.full_name():
                logger.warning(f'Possible name mismach: {row["Name"]} vs. {student.full_name()}')
            self[student.full_name()] = student

    def check_companions(self):
        """
        """
        for student in self.values():
            companion_full_name = student.companion_full_name()
            if companion_full_name is None:
                continue
            try:
                companion = self[companion_full_name]
            except KeyError:
                logger.error(f'Cannot find {companion_full_name} to match with {student.full_name()}')
                continue
            if companion.companion_full_name() != student.full_name():
                logger.error(f'Companion mismatch {student.full_name()} -> {companion_full_name} -> {companion.companion_full_name()}')
                continue
            if student.macro_group != companion.macro_group:
                logger.error(f'Group mismatch: {student.full_name()} {student.macro_group} <-> {companion.full_name()} {companion.macro_group}')



if __name__ == '__main__':
    db = DataBase('lab1_20221009_edit.xlsx')
    db.check_companions()