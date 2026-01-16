from simphony_mayavi.cuds.api import VTKMesh, VTKLattice, VTKParticles


def adapt2cuds(data_set, name='CUDS container', kind=None, rename_arrays=None):
    """ Adapt a TVTK dataset to a CUDS container.

    Parameters
    ----------
    data_set : tvtk.Dataset
        The dataset to import and wrap into CUDS container.

    name : string
        The name of the CUDS container. Default is 'CUDS container'.

    kind : {'mesh', 'lattice', 'particles'}
        The kind of the container to return. Default is None, where
        the function will use some heuristics to infer the most
        appropriate type of CUDS container to return

    rename_array : dict
        Dictionary mapping the array names used in the dataset object
        to their related CUBA keywords that will be used in the returned
        CUDS container.

        .. note::

          When set a shallow copy of the input data_set is created and
          used by the related vtk -> cuds wrapper.

    """
    if rename_arrays is not None and len(rename_arrays) != 0:
        renamed = data_set.new_instance()
        renamed.shallow_copy(data_set)
        for name in rename_arrays:
            if renamed.point_data.has_array(name):
                array = renamed.point_data.get_array(name)
                array.name = rename_arrays[name].name
            if renamed.cell_data.has_array(name):
                array = renamed.cell_data.get_array(name)
                array.name = rename_arrays[name].name
        data_set = renamed
    if kind == u'mesh':
        container = VTKMesh.from_dataset(name, data_set=data_set)
    elif kind == u'lattice':
        container = VTKLattice.from_dataset(name, data_set=data_set)
    elif kind == u'particles':
        container = VTKParticles.from_dataset(name, data_set=data_set)
    elif kind is None:
        for constructor in [
                VTKParticles.from_dataset,
                VTKMesh.from_dataset,
                VTKLattice.from_dataset]:
            try:
                container = constructor(name, data_set=data_set)
            except TypeError:
                continue
            else:
                break
        else:
            message = "Cannot guess appropriate CUDS container for: {!s}"
            raise TypeError(message.format(data_set))
    else:
        message = "Unknown type of CUDS container: {}"
        raise ValueError(message.format(kind))
    return container