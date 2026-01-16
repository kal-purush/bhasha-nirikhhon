import Graph from './Graph';
import Vertex from './Vertex';
import Edge from './Edge';

describe('Graph', () => {
  test('check Graph base functionals', () => {
    const graph = new Graph<string>();
    const vertexA = new Vertex('A', 'A');
    const vertexB = new Vertex('B', 'B');
    graph.addVertex(vertexA);
    graph.addVertex(vertexB);

    graph.addEdge(new Edge(vertexA, vertexB, 0));

    expect(graph.getVertexByKey(vertexA.key).key).toBe('A');
    expect(graph.getVertexByKey(vertexB.key).key).toBe('B');
    graph.neighborsForKey(vertexA.key).forEach(item => expect(item).toBe(vertexB));
    expect(graph.toString()).toBe('A,B');
  });
});