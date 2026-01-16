#!/usr/bin/env python
# -*- coding: ISO-8859-1 -*-

import wx
import maingui


class Update(maingui.dlgUpdate):
    # constructor

    def __init__(self, parent):
        # initialize parent class
        maingui.dlgUpdate.__init__(self, parent)

    def LoadText(self, sVersion, sReason):
        text = self.textUpdate
        text.AppendText("Nieuwe versie : ")
        text.SetDefaultStyle(wx.TextAttr('#008502'))
        text.AppendText(str(sVersion) + '\n')
        text.SetDefaultStyle(wx.TextAttr(wx.BLACK))
        text.AppendText("Veranderingen : \n")
        for row in sReason:
            #            sPoep = str((row[0]))
            sRegel = str(row)
            text.AppendText('- ' + sRegel + '\n')

    def onbtnAfsluitenClick(self, event):
        self.EndModal(wx.ID_OK)