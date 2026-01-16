# References
'''
https://github.com/FedericoStra/cython-package-example/blob/master/setup.py
https://github.com/thewtex/cython-cmake-example/blob/master/setup.py
'''

import os
from setuptools import setup
from distutils.extension import Extension
from Cython.Distutils import build_ext

import numpy

mpi_compile_args = os.popen("mpic++ --showme:compile").read().strip().split(' ')
mpi_link_args = os.popen("mpic++ --showme:link").read().strip().split(' ')

ext_modules = [
    Extension("mycythonlibs",
              sources=["lib/simplelibrary.pyx", "/home/vibhatha/github/mycythonlib/cpp/src/simplelibrary.cpp"],
              include_dirs=[numpy.get_include(), "/home/vibhatha/github/mycythonlib/cpp/include/simplelibrary.h"],
              language='c++',
              extra_compile_args=mpi_compile_args,
              extra_link_args=mpi_link_args,
              )
]



setup(
    name="mycythonlib",
    cmdclass={"build_ext": build_ext},
    ext_modules=ext_modules
)

###########################################
#
# from distutils.core import setup
# from distutils.extension import Extension
# from Cython.Distutils import build_ext
#
# import os
#
# os.environ['CC'] = '/home/vibhatha/software/openmpi/build/bin/mpic++'
#
# import numpy
#
# setup(
#     cmdclass={'build_ext': build_ext},
#     ext_modules=[Extension("multiply",
#                            sources=["multiply.pyx", "c_multiply.c"],
#                            include_dirs=[numpy.get_include()])],
# )