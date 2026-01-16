from wknml import NMLParameters, Group, Edge, Node, Tree, NML, Branchpoint, Comment
from wknml.nml_generation import generate_graph, generate_nml

def test_generate_nml():
        nodes = [Node(id=1, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2], inVp=0,
                  inMag=1, bitDepth=8, interpolation=True),
                 Node(id=2, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2],
                      inMag=1, bitDepth=8, interpolation=True),
                 Node(id=3, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2],
                      bitDepth=8, interpolation=True),
                 Node(id=1, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2], interpolation=True),
                 Node(id=2, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2]),
                 Node(id=3, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2]),
                 Node(id=1, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2]),
                 Node(id=2, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2]),
                 Node(id=3, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2]),
                 Node(id=4, radius=2.0, position=[1.0, 2.0, 3.0], rotation=[0.2, 0.2, 0.2]),
                 ]

        edges = [Edge(1, 2), Edge(3, 1), Edge(2, 3),
                 Edge(1, 2), Edge(3, 1), Edge(2, 3),
                 Edge(1, 2), Edge(3, 1), Edge(2, 3), Edge(1, 4), Edge(4, 2)]

        trees = [Tree(nodes=nodes[0:3],
                      edges=edges[0:3],
                      id=1,
                      name="tree1",
                      groupId=1,
                      color=(0.4, 0.3, 0.8, 1.0)),
                 Tree(nodes=nodes[3:6],
                      edges=edges[3:6],
                      id=2,
                      name="tree1",
                      groupId=2,
                      color=(0.4, 0.3, 0.8, 1.0)),
                 Tree(nodes=nodes[6:],
                      edges=edges[6:],
                      id=3,
                      name="tree1",
                      groupId=2,
                      color=(0.4, 0.3, 0.8, 1.0))
                 ]

        branchpoints = [Branchpoint(1, 129), Branchpoint(2, 42), Branchpoint(4, 1337)]

        comments = [Comment(1, "test"), Comment(2, "hax0r"), Comment(4, "admin admin")]

        groups = [Group(id=1, name="group1", children=[]), Group(id=2, name="group2", children=[])]

        parameters = NMLParameters(
                        name="test_dataset",
                        scale=(11, 11, 25),
                        offset=(1, 1, 1),
                        time=1337,
                        editPosition=(3, 6, 0),
                        editRotation=(4, 2, 0),
                        zoomLevel=100
                      )

        expected_nml = NML(parameters=parameters,
                           trees=trees,
                           branchpoints=branchpoints,
                           comments=comments,
                           groups=groups)

        graphs = []

        graph = generate_graph(expected_nml)
        test_result_nml = generate_nml(graph)

        assert test_result_nml == expected_nml


if __name__ == "__main__":
    test_generate_nml()