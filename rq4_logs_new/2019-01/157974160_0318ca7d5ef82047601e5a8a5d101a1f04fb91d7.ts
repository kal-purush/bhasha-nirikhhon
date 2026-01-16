interface IEdge {
  getKey: () => string;
  reverse: () => this;
  toString: () => string;
}

class Edge implements IEdge {
  private startVertex: number;
  private endVertex: number;
  private weight: number;

  constructor(startVertex: number, endVertex: number, weight = 0) {
    this.startVertex = startVertex;
    this.endVertex = endVertex;
    this.weight = weight;
  }

  getKey() {
    const startVertexKey = this.startVertex.getKey();
    const endVertexKey = this.endVertex.getKey();

    return `${startVertexKey}_${endVertexKey}`;
  }

  reverse() {
    const tmp = this.startVertex;
    this.startVertex = this.endVertex;
    this.endVertex = tmp;

    return this;
  }

  toString() {
    return this.getKey();
  }
}

export default Edge;