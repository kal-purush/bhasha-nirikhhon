#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  klasa_uczen.py

import os
from peevee import *

######### MODEL
baza = SqliteDatabase(baza_plik)
class BazaModel(Model):
    class Meta:
        database = baza
        
class Klasa(BazaModel):
    nazwa = CharField(null=Falsee)
    roknaboru =
    
def main(args):
    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main(sys.argv))