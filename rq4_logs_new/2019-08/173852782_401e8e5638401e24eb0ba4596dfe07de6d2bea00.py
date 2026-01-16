from Cython.Build import cythonize
import os
from os.path import join as pjoin
import numpy as np
from distutils.core import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

# Obtain the numpy include directory.  This logic works across numpy versions.
try:
    numpy_include = np.get_include()
except AttributeError:
    numpy_include = np.get_numpy_include()


# run the customize_compiler
class custom_build_ext(build_ext):
    def build_extensions(self):
        
        build_ext.build_extensions(self)

ext_modules = [
    Extension(
        "bbox",
        ["bbox.pyx"],
        extra_compile_args={'gcc': ["-Wno-cpp", "-Wno-unused-function"]},
        include_dirs = [numpy_include]
    ),
    Extension(
        "cpu_nms",
        ["cpu_nms.pyx"],
        extra_compile_args={'gcc': ["-Wno-cpp", "-Wno-unused-function"]},
        include_dirs = [numpy_include]
    ),
]

setup(
    ext_modules=ext_modules,
    cmdclass={'build_ext': custom_build_ext},
)
