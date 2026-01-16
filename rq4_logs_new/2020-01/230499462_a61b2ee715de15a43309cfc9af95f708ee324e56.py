from cx_Freeze import setup, Executable

setup(
    name = "HER2",
    version = "0.2",
    description = "HER2",
    executables = [Executable("MyWindow2.py")]
)