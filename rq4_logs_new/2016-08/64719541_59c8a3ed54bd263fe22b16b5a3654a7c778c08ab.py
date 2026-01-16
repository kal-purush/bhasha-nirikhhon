from glob import glob
from importlib import import_module
from os.path import basename, dirname, isfile, join

from app.commands.registry import registry


module_filepaths = glob(join(dirname(__file__), '*.py'))

for filepath in module_filepaths:
    module = basename(filepath)[:-3]

    import_module("app.commands." + module)


__all__ = [registry]