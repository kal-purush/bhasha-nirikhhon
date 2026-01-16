# -*- coding: utf-8 -*-
"""
/***************************************************************************
 irods_qgisDialog
                                 A QGIS plugin
 Connect to irods via qgis
                             -------------------
        begin                : 2014-12-23
        git sha              : $Format:%H$
        copyright            : (C) 2014 by Amit Juneja / BCF
        email                : amitj@email.arizona
 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/
"""

import os

from PyQt4 import QtGui, uic
from PyQt4.QtGui import *
from PyQt4.QtCore import *


FORM_CLASS, _ = uic.loadUiType(os.path.join(
    os.path.dirname(__file__), 'about_us.ui'))


class about_us(QtGui.QDialog, FORM_CLASS):


    def __init__(self,parent=None):
        """Constructor."""
        super(about_us, self).__init__(parent)

        #initialize dicts and lists for the plugin
        self.setupUi(self)
       
