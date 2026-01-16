#!/usr/bin/python
# -*- coding: utf-8 -*-

import gi
gi.require_version('Gtk', '3.0')
gi.require_version('Handy', '0.0')
from gi.repository import Gtk, GdkPixbuf, Handy
import os


APP_VERSION = "0.1"
APP_NAME = "Score Keeper"
APP_ID = "org.airon.scorekeeper"

ABS_PATH = os.path.dirname(os.path.abspath(__file__)) + \
    "/" if os.path.dirname(os.path.abspath(__file__)) else ""


class Program(Gtk.Window):
    def __init__(self, names):
        Gtk.Window.__init__(self, title=APP_NAME)
        self.set_default_size(800, 600)
        self.set_position(Gtk.WindowPosition.CENTER)

        hb = Handy.HeaderBar()
        hb.set_show_close_button(True)
        hb.props.title = APP_NAME
        self.set_titlebar(hb)

        button = Gtk.Button()
        button.add(Gtk.Image.new_from_icon_name(
            "document-new", Gtk.IconSize.BUTTON))
        button.connect("clicked", self.on_new_clicked)  # , args)
        hb.pack_start(button)

        button = Gtk.Button()
        button.add(Gtk.Image.new_from_icon_name(
            "help-symbolic", Gtk.IconSize.BUTTON))
        button.connect("clicked", self.on_about_clicked)
        hb.pack_end(button)

        scrolled = Gtk.ScrolledWindow()
        scrolled.set_policy(Gtk.PolicyType.NEVER, Gtk.PolicyType.AUTOMATIC)
        self.add(scrolled)

        self.vbox = Gtk.Box(orientation=Gtk.Orientation.VERTICAL)
        Gtk.StyleContext.add_class(self.vbox.get_style_context(), "linked")
        scrolled.add(self.vbox)

        options = ["Won", "Lost", "Draw"]

        self.group = []
        for name in range(len(names)):
            self.group = Handy.PreferencesGroup()
            self.group.set_title(names[name])
            for i in range(3):
                self.adjustment = Gtk.Adjustment()
                self.adjustment.set_value(0)
                self.adjustment.set_lower(-10000)
                self.adjustment.set_upper(10000)
                self.adjustment.set_step_increment(1)
                self.adjustment.set_page_increment(10)
                self.adjustment.set_page_size(0)
                self.player_spin = Gtk.SpinButton()
                self.player_spin.set_adjustment(self.adjustment)
                self.player_spin.set_digits(0)
                self.player_spin.set_value(0)

                self.row = Handy.ActionRow()
                self.row.set_title(options[i])
                self.row.add_action(self.player_spin)
                self.row.set_activatable_widget(self.player_spin)
                self.group.add(self.row)

            self.vbox.pack_start(self.group, True, True, 0)

    def on_new_clicked(self, args):
        dialog = Gtk.MessageDialog(message_type=Gtk.MessageType.WARNING, transient_for=self)
        dialog.props.text = "New session is to be implemented"
        dialog.add_button("Ok", Gtk.ResponseType.CLOSE) #Gtk.ButtonsType.OK, Gtk.ResponseType.CLOSE)
        dialog.format_secondary_text(
            "By now, in order to restart the app you must close the app and open it again.")
        response = dialog.run()

        dialog.destroy()


    def on_about_clicked(self, action):
        aboutdialog = Gtk.AboutDialog(modal=True, transient_for=self)
        aboutdialog.set_program_name(APP_NAME)
        aboutdialog.set_version(APP_VERSION)
        aboutdialog.set_license_type(Gtk.License.GPL_3_0)
        aboutdialog.set_copyright("Copyright \xa9 2020 Michael Moroni")
        aboutdialog.set_comments("Keep track of your game points")
        aboutdialog.set_authors(["Michael Moroni"])
        aboutdialog.set_documenters(["Michael Moroni"])
        aboutdialog.set_website("http://github.com/airon90/scoreKeeper")
        aboutdialog.set_website_label("GitHub repository")
        aboutdialog.set_translator_credits("Michael Moroni")
#        aboutdialog.set_logo(
#            GdkPixbuf.Pixbuf.new_from_file_at_size(ABS_PATH + "logo.png", 64, 64))
        aboutdialog.connect('response', lambda dialog, data: dialog.destroy())
        aboutdialog.show_all()


def main():
    names = []

    player = input("Players: ")
    for pl in range(int(player)):
        name = input("Name player " + str(int(pl) + 1) + ": ")
        names.append(name)

    app = Program(names)
    app.connect("delete-event", Gtk.main_quit)
    app.show_all()
    Gtk.main()


if __name__ == '__main__':
    main()