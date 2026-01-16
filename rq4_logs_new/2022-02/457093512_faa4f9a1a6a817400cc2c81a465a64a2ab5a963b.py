#! /usr/bin/env python3
'''
Use PyAutoGUI to determine cursor position and screen size.

After your chosen URL opens, move the cursor to the desired position. The
default time is 5 s.

The result can be used in other sripts requiring PyAutoGUI.
'''

from matplotlib import Path
import webbrowser
import time

import pyautogui as pyg
import datasense as ds


def main():
    output_url = 'pyautogui_position_report.html'
    url = ('https://docs.python.org/3/')
    header_title = 'pyautogui position'
    header_id = 'pyautogui-position'
    original_stdout = ds.html_begin(
        output_url=output_url,
        header_title=header_title,
        header_id=header_id
    )
    ds.script_summary(
        script_path=Path(__file__),
        action='started at'
    )
    webbrowser.open(url=url)
    time.sleep(5)
    width, height = pyg.size()
    print('screen width, height:', width, height)
    print()
    currentx, currenty = pyg.position()
    print('current x, y:', currentx, currenty)
    print()
    ds.html_end(
        original_stdout=original_stdout,
        output_url=output_url
    )


if __name__ == '__main__':
    main()