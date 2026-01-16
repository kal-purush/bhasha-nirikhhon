import pip

_all_ = [
    "pip",
    "docutils",
    "kivy",
    "pytest",
    "pyinstaller",
    "wheel",
    "setuptools",
    "cython"
]

windows = [
    "kivy.deps.sdl2",
    "pypiwin32",
    "kivy.deps.glew",
    "kivy.deps.angle",
    "kivy.deps.gstreamer"
]

linux = [

]

darwin = [
    "pkg-config",
    "sdl2",
    "sdl2_image",
    "sdl2_ttf",
    "sdl2_mixer",
    "gstreamer"
]


def install(packages):
    for package in packages:
        pip.main(['install', package])


if __name__ == '__main__':

    from sys import platform

    install(_all_)
    if platform == 'windows':
        install(windows)
    if platform.startswith('linux'):
        install(linux)
    if platform == 'darwin':  # MacOS
        install(darwin)