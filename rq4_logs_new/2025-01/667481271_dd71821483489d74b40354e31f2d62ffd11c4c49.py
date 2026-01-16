import win32com.client
import os
from pathlib import Path


def step_saver(file):
    inventor = win32com.client.Dispatch("Inventor.Application")
    inventor.Visible = False
    doc = inventor.Documents.Open(file)
    location = Path(file).parent
    new_file = file[len(str(location)) + 1:].split('.')[0] + '.x_t'
    new_file = os.path.join(location, new_file)
    doc.SaveAs(new_file, True)
    doc.Close()
    return None
    # inventor.Visible = True  # Opcjonalnie pokaż okno aplikacji


def main():
    pass


if __name__ == '__main__':
    step_saver('N:\\1740023\\0004_A\\1740023-0004.ipt')
    main()