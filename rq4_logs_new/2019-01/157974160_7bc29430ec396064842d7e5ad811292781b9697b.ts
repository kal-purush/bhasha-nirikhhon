import { IVertex } from './Vertex';

export interface IEdge<T> {
  from: IVertex<T>
  to: IVertex<T>
  weight: number
  getFrom: () => IVertex<T>
  getTo: () => IVertex<T>
  getKey: () => string
  revert: () => this
};

class Edge<T> implements IEdge<T> {
  public from: IVertex<T>;
  public to: IVertex<T>;
  public weight: number;

  constructor(from: IVertex<T>, to: IVertex<T>, weight: number) {
    this.from = from;
    this.to = to;
    this.weight = weight;
  }

  getFrom() {
    return this.from;
  }

  getTo() {
    return this.to;
  }

  getKey() {
    return `${this.from.key}_${this.to.key}`;
  }

  revert() {
    const tmp = this.from;
    this.from = this.to;
    this.to = tmp;

    return this;
  }
}

export default Edge;