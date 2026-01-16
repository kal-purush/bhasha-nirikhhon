# -*- coding: utf-8 -*-
#
#  Copyright (C) 2017 - Marc Volker Dickmann
#
#  This program is free software; you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation; either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, see <http://www.gnu.org/licenses/>.

import os

import gi
gi.require_version('Gedit', '3.0')
gi.require_version('Gtk', '3.0')
from gi.repository import GObject, Gio, GLib, Gtk, GtkSource, Gedit

class ShowSpacesPlugin(GObject.Object, Gedit.ViewActivatable):
    __gtype_name__ = "ShowSpacesPlugin"

    view = GObject.Property(type=Gedit.View)

    def __init__(self):
        GObject.Object.__init__(self)

    def do_activate(self):
        if self.view:
            self.view.set_draw_spaces(GtkSource.DrawSpacesFlags.SPACE | GtkSource.DrawSpacesFlags.TAB | GtkSource.DrawSpacesFlags.LEADING | GtkSource.DrawSpacesFlags.TRAILING)

    def do_deactivate(self):
        if self.view:
            self.view.set_draw_spaces(0)