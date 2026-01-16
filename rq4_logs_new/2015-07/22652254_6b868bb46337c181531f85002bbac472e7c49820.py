from mayavi.core.api import registry

from simphony_mayavi.adapt2cuds import adapt2cuds


def load(filename, name=None, kind=None, rename_arrays=None):
    data_set = _read(filename)
    return adapt2cuds(
        data_set, name, kind, rename_arrays)


def _read(filename):
    """ Find a suitable reader and read in the tvtk.Dataset.
    """
    metasource = registry.get_file_reader(filename)
    if metasource is None:
        message = 'No suitable reader found for file: {}'
        raise RuntimeError(message.format(filename))
    if metasource.factory is None:
        source = metasource.get_callable()()
        source.initialize(filename)
        source.update()
        reader = source.reader
    else:
        message = 'Mayavi reader that requires a scene is not supported : {}'
        raise NotImplementedError(message.format(filename))

    if len(source.outputs) != 1:
        message = 'Only one output is expected from the reader'
        raise RuntimeError(message)

    return reader.output