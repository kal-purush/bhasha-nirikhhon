
import sys
from PyQt5 import QtWidgets, QtGui
import pyperclip
import keyboard
# Dictionary to store clipboard bins
clipboard_bins = {1: "", 2: "", 3: "", 4: "", 5:""}

# Function to copy from clipboard to a bin
def copy_to_bin(bin_number):
    clipboard_content = pyperclip.paste()
    clipboard_bins[bin_number] = clipboard_content
    print(f"Content copied to bin {bin_number}")

# Function to set bin content as current clipboard
def set_active_clipboard(bin_number):
    if clipboard_bins[bin_number]:
        pyperclip.copy(clipboard_bins[bin_number])
        print(f"Bin {bin_number} content set to clipboard")
    else:
        print(f"Bin {bin_number} is empty")

# Register hotkeys for copying to bins
for i in range(1, 5):
    keyboard.add_hotkey(f'ctrl+alt+{i}', lambda i=i: copy_to_bin(i))

# Register hotkeys for setting active clipboard
for i in range(1, 5):
    keyboard.add_hotkey(f'alt+shift+{i}', lambda i=i: set_active_clipboard(i))

# Function to safely exit
def exit_app():
    QtWidgets.QApplication.quit()

# PyQt5 System Tray Application
app = QtWidgets.QApplication(sys.argv)
app.setQuitOnLastWindowClosed(False)

# Create the icon
icon = QtGui.QIcon(r"C:\Users\mikev\Downloads\Five_Bin\V_orange.png")

# Create the tray
tray = QtWidgets.QSystemTrayIcon()
tray.setIcon(icon)
tray.setVisible(True)

# Create the menu
menu = QtWidgets.QMenu()
exit_action = menu.addAction("Exit")
exit_action.triggered.connect(exit_app)


# Add the menu to the tray
tray.setContextMenu(menu)

# Run the application
sys.exit(app.exec_())

