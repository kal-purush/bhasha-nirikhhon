#!/usr/bin/python3
import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk

__author__ = "Alex Kalinkin"


class AboutWindow(Gtk.Window):
    def __init__(self, T):
        Gtk.Window.__init__(self, title=T.MENU_ITEM_ABOUT + ' Gtk3')

        # Hide window when clicking close button in window header
        self.connect('delete-event', lambda w, e: w.hide() or True)

        self.set_default_size(320, 240)
        self.set_position(Gtk.WindowPosition.CENTER)

        # TODO: Set icon 'help-about
        # icon = Gtk.icon_theme_get_default().load_icon('help-about')
        # self.set_icon(icon)

        v_box = Gtk.Box(orientation=Gtk.Orientation.VERTICAL, spacing=3)

        label = Gtk.Label()
        label.set_markup("About program text.\nWith multiple lines.\nAnd with "
                         "<a href='http://kalinkin.info'>Author web site</a>")
        label.set_justify(Gtk.Justification.LEFT)
        v_box.pack_start(label, True, True, 0)

        button_close = Gtk.Button(label="Close")
        button_close.set_property("height-request", 15)
        button_close.connect("clicked", self.button_close_clicked_handler)

        v_box.pack_start(button_close, True, True, 0)

        self.add(v_box)

    def show(self):
        print("AboutWindow.show()")
        self.show_all()

    def button_close_clicked_handler(self, widget):
        print("AboutWindow.button_close_clicked_handler")
        self.hide()  # destroy()