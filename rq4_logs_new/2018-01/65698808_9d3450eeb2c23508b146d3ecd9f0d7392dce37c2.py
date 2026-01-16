#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gettext
import unittest
import time
from lib import executeTestCase
from lib import utils
from lib import runner
from lib import Dock
from lib import DbusLauncher
from dogtail.tree import root

casename = "all-6219:右键驻留"

class Dock_IconMenuDock(unittest.TestCase):
    caseid = '282062'
    @classmethod
    def setUpClass(cls):
        cls.dock = Dock()
        cls.dbus_launcher = DbusLauncher()
        cls.geidticonname = "Text Editor"
        cls.geidtwindowname = cls.dock.string_Text_Editor_Window1
        cls.ddedockobject = utils.getDdeDockObject()

        if utils.dock.displaymode_fashion != utils.getDdeDockDisplayMode():
            utils.setDdeDockDisplayMode(utils.dock.displaymode_fashion)

        if utils.dock.position_bottom != utils.getDdeDockPosition():
            utils.setDdeDockPosition(utils.dock.position_bottom)

    @classmethod
    def tearDownClass(cls):
        if utils.dock.displaymode_fashion != utils.getDdeDockDisplayMode():
            utils.setDdeDockDisplayMode(utils.dock.displaymode_fashion)

        if utils.dock.position_bottom != utils.getDdeDockPosition():
            utils.setDdeDockPosition(utils.dock.position_bottom)

    def testOpenGeditFromLauncher(self):
        self.dbus_launcher.Show()
        try:
            launcherwin = utils.findWindow("dde-launcher")
        except:
            self.assertTrue(False, "Can't open dde-launcher")

        utils.keyTypeString("gedit")
        utils.keySingle(utils.k.enter_key)

        try:
            geidtwin = utils.findWindow(self.geidtwindowname, comparetype="notequal")
            self.assertTrue(True)
        except:
            self.assertTrue(False, "Can't find window: %s" % self.geidtwindowname)

        time.sleep(2)

    def testDockGedit(self):
        try:
            icongeidt = self.ddedockobject.child(self.geidticonname)
        except:
            self.assertTrue(False, "Can't find the geidt icon on the Dock")

        icongeidt.click(3)

        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.enter_key)

    def testUnDockGedit(self):
        try:
            icongeidt = self.ddedockobject.child(self.geidticonname)
        except:
            self.assertTrue(False, "Can't find the geidt icon on the Dock")

        icongeidt.click(3)

        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.down_key)
        utils.keySingle(utils.k.enter_key)

    def testCloseGedit(self):
        geidtwin = utils.findWindow(self.geidtwindowname, comparetype="notequal")
        utils.closeWindow(geidtwin)
        geidtwin_later = utils.findWindow(self.geidtwindowname, mode="nowait", comparetype="notequal")
        self.assertTrue(None == geidtwin_later)

    def testExpectResult(self):
        try:
            icongeidt = self.ddedockobject.child(self.geidticonname)
            self.assertTrue(True)
        except:
            self.assertTrue(False, "Can't find geidt icon on the Dock")

    def testDragGeditIconToDesktop(self):
        icongeidt = self.ddedockobject.child(self.geidticonname)
        fromXY = utils.getDockIconCenterPoint(icongeidt)
        utils.mouseDrag(fromXY, (fromXY[0], fromXY[1] - 100))

        ddedock = utils.getDdeDockObject()
        try:
            icongeidt_later = ddedock.child(self.geidticonname)
        except:
            self.assertTrue(True)

    def testExChangeDisplayMode(self):
        if utils.getDdeDockDisplayMode() == utils.dock.displaymode_fashion:
            utils.setDdeDockDisplayMode(utils.dock.displaymode_efficient)
        elif utils.getDdeDockDisplayMode() == utils.dock.displaymode_efficient:
            utils.setDdeDockDisplayMode(utils.dock.displaymode_fashion)

    def suite():
        suite = unittest.TestSuite()
        suite.addTest(Dock_IconMenuDock('testOpenGeditFromLauncher'))
        suite.addTest(Dock_IconMenuDock('testDockGedit'))
        suite.addTest(Dock_IconMenuDock('testCloseGedit'))
        suite.addTest(Dock_IconMenuDock('testExpectResult'))
        suite.addTest(Dock_IconMenuDock('testDragGeditIconToDesktop'))

        # change display mode
        suite.addTest(Dock_IconMenuDock('testExChangeDisplayMode'))

        suite.addTest(Dock_IconMenuDock('testOpenGeditFromLauncher'))
        suite.addTest(Dock_IconMenuDock('testDockGedit'))
        suite.addTest(Dock_IconMenuDock('testCloseGedit'))
        suite.addTest(Dock_IconMenuDock('testExpectResult'))
        suite.addTest(Dock_IconMenuDock('testUnDockGedit'))

        return suite

if __name__ == "__main__":
    unittest.installHandler()
    LOCALE_DIR = os.path.abspath("./lib/locale")
    gettext.install('dsystem', LOCALE_DIR)
    executeTestCase.runTest(Dock_IconMenuDock)