import os
from pymsbuild import *

METADATA = {
    "Metadata-Version": "2.1",
    "Name": "pymsbuild-rust",
    "Version": "0.0.1",
    "Author": "Steve Dower",
    "Author-email": "steve.dower@python.org",
    "Home-page": "https://github.com/zooba/pymsbuild-rust",
    "Project-url": [
        "Bug Tracker, https://github.com/zooba/pymsbuild-rust/issues",
    ],
    "Summary": "A pymsbuild extension for compiling native modules written in Rust.",
    "Description": File("README.md"),
    "Description-Content-Type": "text/markdown",
    "Keywords": "build,pep-517,msbuild,packaging,rust",
    "Classifier": [
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: Microsoft :: Windows",
        "Operating System :: POSIX :: Linux",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Build Tools",
        "Topic :: Software Development :: Compilers",
        "Topic :: Utilities",
    ],
    "Requires-External": "rust",
    "Requires-Dist": [
        "pymsbuild>=1.0.0b1",
    ],
    "WheelTag": "py3-none-any",
}

PACKAGE = Package(
    "pymsbuild_rust",
    PyFile("pymsbuild_rust/*.py"),
    File("pymsbuild_rust/targets/*", name="targets/*"),
)

def init_METADATA():
    version = os.getenv("BUILD_BUILDNUMBER")
    ghref = os.getenv("GITHUB_REF")
    if ghref:
        version = ghref.rpartition("/")[2]
    if version:
        METADATA["Version"] = version

def init_PACKAGE(tag=None):
    if tag is None:
        return
    with open("pymsbuild_rust/__init__.py", "r", encoding="utf-8") as f:
        content = f.read()
    content = content.replace("%VERSION%", METADATA["Version"])
    with open("pymsbuild_rust/__init__.py.ver", "w", encoding="utf-8") as f:
        f.write(content)
    PACKAGE.members.append(RemoveFile(PyFile, "pymsbuild_rust/__init__.py"))
    PACKAGE.members.append(PyFile("pymsbuild_rust/__init__.py.ver", name="__init__.py"))