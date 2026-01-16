import { Node } from '@xyflow/react';
import { distance } from './distance';
import { AlgoFunction } from '../../utils/types';
import { pathToEdges } from '../mappers/path-to-edges';
import {
  CURRENT_MIN_COLOR,
  CURRENT_TESTED_COLOR,
  FINAL_COLOR,
} from '../../utils/consts';
import { prepareNodesForAlgorithms } from '../mappers/prepare-nodes-for-algorithms';

function swap(array: string[], i: number, j: number) {
  const tmp = array[i];
  array[i] = array[j];
  array[j] = tmp;
}

function* lexicographic(a: string[]) {
  let j, k, l;
  const n = a.length - 1;
  while (true) {
    yield a;
    j = n - 1;
    while (a[j] >= a[j + 1]) {
      j--;
    }
    if (j < 0) {
      return;
    }
    l = n;
    while (a[j] >= a[l]) {
      l--;
    }
    swap(a, j, l);
    k = j + 1;
    l = n;
    while (k < l) {
      swap(a, k, l);
      k++;
      l--;
    }
  }
}

export const bruteForce: AlgoFunction = function* (nodes, omitIntermediate) {
  const { ids, nodeMap } = prepareNodesForAlgorithms(nodes);
  const nodesToFindPath = ids.slice(1).sort();
  let currentMinLength = Number.POSITIVE_INFINITY;
  let currentMinPath: string[] = [];
  for (const path of lexicographic(nodesToFindPath)) {
    const fullPath = [ids[0], ...path, ids[0]];
    let currentLength = 0;
    for (let i = 0; i < fullPath.length - 1; i++) {
      const currentNode = fullPath[i];
      const nextNode = fullPath[i + 1];
      currentLength += distance(
        nodeMap.get(currentNode),
        nodeMap.get(nextNode)
      );
    }
    if (!omitIntermediate) {
      yield [
        ...pathToEdges(currentMinPath, CURRENT_MIN_COLOR),
        ...pathToEdges(fullPath, CURRENT_TESTED_COLOR),
      ];
    } else {
      yield [];
    }
    if (currentLength < currentMinLength) {
      currentMinLength = currentLength;
      currentMinPath = fullPath;
    }
  }
  const finalResult = pathToEdges(currentMinPath, FINAL_COLOR);
  yield finalResult;
  return finalResult;
};