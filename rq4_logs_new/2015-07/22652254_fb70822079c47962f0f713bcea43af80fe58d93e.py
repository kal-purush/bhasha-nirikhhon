from numpy import array

from simphony.cuds.mesh import Point, Cell, Edge, Face
from simphony.core.data_container import DataContainer
from simphony_mayavi.cuds.api import VTKMesh


points = array([
    [0, 0, 0], [1, 0, 0], [0, 1, 0], [0, 0, 1],
    [2, 0, 0], [3, 0, 0], [3, 1, 0], [2, 1, 0],
    [2, 0, 1], [3, 0, 1], [3, 1, 1], [2, 1, 1]],
    'f')

cells = [
    [0, 1, 2, 3],  # tetra
    [4, 5, 6, 7, 8, 9, 10, 11]]  # hex

faces = [[2, 7, 11]]
edges = [[1, 4], [3, 8]]

mesh = VTKMesh('example')

# add points
uids = [
    mesh.add_point(
        Point(coordinates=point, data=DataContainer(TEMPERATURE=index)))
    for index, point in enumerate(points)]

# add edges
edge_uids = [
    mesh.add_edge(
        Edge(points=[uids[index] for index in element]))
    for index, element in enumerate(edges)]

# add faces
face_uids = [
    mesh.add_face(
        Face(points=[uids[index] for index in element]))
    for index, element in enumerate(faces)]

# add cells
cell_uids = [
    mesh.add_cell(
        Cell(points=[uids[index] for index in element]))
    for index, element in enumerate(cells)]


if __name__ == '__main__':
    from simphony.visualisation import mayavi_tools

    # Visualise the Mesh object
    mayavi_tools.show(mesh)