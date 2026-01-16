# !/usr/bin/env python
# -*- coding: utf-8 -*-

from commun.ui.public.mondon_widget import MondonWidget


class BobineFille(MondonWidget):

    def __init__(self, parent=None, code=0, laize=0, lenght=0, color="", alerte=False, gr="0g", pose=0):
        super(BobineFille, self).__init__(parent=parent)
        self.code = code
        self.laize = laize
        self.lenght = lenght
        self.color = color
        self.alert = alerte
        self.gr = gr
        self.pose = pose

    def __str__(self):
        return "B{}({}, {}, {}m, {} poses, {}, {})".format(self.code,
                                                           self.color.capitalize(),
                                                           self.laize,
                                                           self.lenght,
                                                           self.pose,
                                                           self.gr,
                                                           self.alert)