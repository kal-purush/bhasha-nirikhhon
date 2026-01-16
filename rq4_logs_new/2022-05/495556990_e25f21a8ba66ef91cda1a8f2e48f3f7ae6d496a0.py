from spack import *

class Bricks(CMakePackage):

    """Bricks is a data layout and code generation framework, enabling performance-portable stencil computations across a multitude of architectures."""

    # url for your package's homepage here.
    homepage = "https://bricks.run/"
    git = 'https://github.com/CtopCsUtahEdu/bricklib.git'

    # List of GitHub accounts to notify when the package is updated.
    maintainers = ['ztuowen', 'drhansj']

    version('r0.1', branch='r0.1')

    variant('cuda', default=False, description='Build bricks with CUDA enabled')

    # Building a variant of cmake without openssl is to match how the
    # ECP E4S project builds cmake in their e4s-base-cuda Docker image
    depends_on('cmake~openssl', type='build')
    depends_on('autoconf', type='build')
    depends_on('automake', type='build')
    depends_on('libtool', type='build')
    depends_on('opencl-clhpp', when='+cuda')
    depends_on('cuda', when='+cuda')
    depends_on('mpi')

    def cmake_args(self):
        args = []

        return args

    def flag_handler(self, name, flags):
        if name in ['cflags', 'cxxflags', 'cppflags']:
            # There are many vector instrinsics used in this package. If
            # the package is built on a native architecture, then it likely
            # will not run (illegal instruction fault) on a less feature-
            # rich architecture.
            # If you intend to use this package in an architecturally-
            # heterogeneous environment, then the package should be build
            # with "target=x86_64". This will ensure that all Intel
            # architectures can use the libraries and tests in this
            # project by forceing the AVX512F flag in gcc.
            if name == 'cxxflags' and self.spec.target == 'x86_64':
                flags.append('-mavx512f')
            return (None, flags, None)
        return(flags, None, None)