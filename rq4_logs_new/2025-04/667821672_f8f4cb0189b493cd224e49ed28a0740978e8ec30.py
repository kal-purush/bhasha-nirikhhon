# This script is needed to create the compilation database for the project. It is 
# executed by PlatformIO before the build process starts.
#
# The file compile_commands.json will be created in the build directory and is needed for Linting the project using SonarCube.

import os
Import("env")

# include toolchain paths
env.Replace(COMPILATIONDB_INCLUDE_TOOLCHAIN=True)

# override compilation DB path
env.Replace(COMPILATIONDB_PATH=os.path.join("$BUILD_DIR", "compile_commands.json"))