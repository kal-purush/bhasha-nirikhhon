#!/usr/bin/env python3.5
# coding: utf-8

"""Anticipair."""

import os
import datetime

import pandas

import anticipair.predicteur as predicteur
from anticipair import constante_instal


def run_exemple():
    mesure = 'N2CINQ'

    # Lecture du jeu de données test
    fndat = os.path.join('data', 'data.csv')
    dat = pandas.read_csv(fndat, sep=',', parse_dates=[0, ],
                          index_col=('dateheure',))

    # Préparation des données
    dat['annee'] = dat.index.year
    dat['mois'] = dat.index.month
    dat['jour'] = dat.index.day
    dat['heure'] = dat.index.hour
    dat = dat.dropna()

    # On supprime des valeurs intermédiaires
    dat['2017-01-17 03:00:00':'2017-01-17 07:00:00'] = None
    dat = dat.dropna()
    print(dat)

    # Anticipair
    resultat = None
    pred = predicteur.predicteur(constante_instal.__dict__[mesure],
                                 reset_prediction=True)
    for i, enr in dat.iterrows():
        resultat = pred.Prediction(
            [enr[mesure], enr['heure'], enr['jour'], enr['mois'],
             enr['annee']])

        print(pred.Info_Date(), enr[mesure])
        print(resultat)


if __name__ == '__main__':
    run_exemple()