import sys
from cx_Freeze import setup, Executable

build_exe_options = {"includes": ["os", "bs4", "urllib", "win10toast"]}

base = None
if sys.platform == "win32":
    base = "Win32GUI"


setup(name="Update CSVs",
      version="1",
      description="Updating CSV information",
      options={"build_exe": build_exe_options},
      executables=[Executable("C:\\Users\\Benjamin Princen\\Workspace\\projects\\web_scrapper\\web-scrapper\\apps\\update_csv.py", base=base)])