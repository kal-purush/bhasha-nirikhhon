import { NodeType } from './createNode';

export type EdgeType<T> = {
  weight: number
  getFrom: () => NodeType<T>
  getTo: () => NodeType<T>
  getKey: () => string
  revert: () => void
};

export const createEdge = <T>(
  from: NodeType<T>,
  to: NodeType<T>,
  weight: number
): EdgeType<T> => {
  let startNode = from;
  let endNode = to;

  const getKey = () => `${startNode.key}_${endNode.key}`;

  const getFrom = () => startNode;

  const getTo = () => endNode;

  return {
    weight,
    getFrom,
    getTo,
    getKey,
    revert() {
      const tmp = startNode;
      startNode = endNode;
      endNode = tmp;
    }
  };
};

export default createEdge;