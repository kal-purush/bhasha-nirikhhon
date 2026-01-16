import queue
import traceback
import threading

from iota.adapter import resolve_adapter
from iota.commands.core import GetNodeInfoCommand

from .wallet import Wallet

import gi
gi.require_version('Gtk', '3.0')
from gi.repository import Gtk, GObject, GLib  # noqa: E402

DEFAULT_URI = 'http://astra2261.startdedicated.net:14265'


class MyWindow(Gtk.Window):

    def __init__(self, *args, **kwargs):
        Gtk.Window.__init__(self, title="Iota Wallet", *args, **kwargs)
        self.set_border_width(10)

        self.worker = WorkerThread()

        grid = Gtk.Grid()
        self.add(grid)

        icon = Gtk.Label()
        icon.set_text('Iota')
        icon.set_vexpand(True)
        icon.set_hexpand(True)
        grid.attach(icon, 0, 0, 1, 10)

        self.sub_grid = HomogeneousGrid()
        grid.attach(self.sub_grid, 0, 10, 1, 3)

        self.status_bar = Gtk.Statusbar()
        self.status_bar.set_margin_bottom(0)
        # Get the initial status and then periodically check
        self.update_milestone()
        GObject.timeout_add(15 * 1000, self.update_milestone)
        grid.attach(self.status_bar, 0, 13, 1, 1)

        self.seed_entry = Gtk.Entry()
        self.seed_entry.set_placeholder_text('Seed')
        self.seed_entry.set_visibility(False)
        self.sub_grid.attach(self.seed_entry, 0, 0, 1, 1)

        self.login = Gtk.Button(label='Login')
        self.login.connect('clicked', self.on_button_clicked)
        self.sub_grid.attach(self.login, 0, 1, 1, 2)

        self.login_spinner = Gtk.Spinner()

    def on_button_clicked(self, widget):
        seed = self.seed_entry.get_text()

        # Check seed
        error = ''
        if len(seed) != 81:
            error = 'Error: Seed must be 81 characters'
        else:
            valid_chars = '9ABCDEFGHIJKLMNOPQRSTUVWXYZ'
            if not all(char in valid_chars for char in seed):
                error = 'Error: Seed must contain only A-Z or 9'

        if error:
            print(error)
            widget.set_label(error)
            widget.handler_block_by_func(self.on_button_clicked)
            GObject.timeout_add(3000, self.button_timeout_expired)
            return

        self.wallet = Wallet(
            uri=DEFAULT_URI,
            seed=self.seed_entry.get_text(),
        )

        self.sub_grid.remove(self.login)
        self.sub_grid.attach(self.login_spinner, 0, 1, 1, 2)
        self.login_spinner.start()
        self.show_all()

        self.worker.send(self.wallet.refresh_account, callback=self.after_account)

    def button_timeout_expired(self):
        self.login.set_label('Login')
        self.login.handler_unblock_by_func(self.on_button_clicked)
        # Return False so that this timeout is cancelled
        return False

    def after_account(self, result):
        print(self.wallet.balance)
        self.login_spinner.stop()

    def update_milestone(self):
        response = GetNodeInfoCommand(resolve_adapter(DEFAULT_URI))()
        current_milestone = response['latestSolidSubtangleMilestoneIndex']
        latest_milestone = response['latestMilestoneIndex']

        if current_milestone == latest_milestone:
            status = 'Synced: {milestone}'.format(milestone=current_milestone)
        else:
            status = 'Unsynced: {0}/{1}'.format(current_milestone, latest_milestone)

        self.status_bar.push(0, status)
        # return True so that this continues to be called
        return True


class HomogeneousGrid(Gtk.Grid):
    def __init__(self):
        super().__init__()
        self.set_column_homogeneous(True)
        self.set_row_homogeneous(True)


class WorkerThread:
    def __init__(self):
        self.thread = threading.Thread(target=self._run)
        self.thread.daemon = True
        self.queue = queue.Queue()
        self.thread.start()

    def _run(self):
        while True:
            command, args, callback = self.queue.get()
            try:
                result = command(*args)
                if callback:
                    GLib.idle_add(callback, result)
            except Exception as e:
                traceback.print_exc()

    def send(self, command, args=(), callback=None):
        self.queue.put((command, args, callback))


window = MyWindow(default_height=600, default_width=400)
# Give focus to the login button so that the seed entry field doesn't have focus
window.login.grab_focus()
window.connect("delete-event", Gtk.main_quit)
window.show_all()


def main():
    Gtk.main()


if __name__ == '__main__':
    main()