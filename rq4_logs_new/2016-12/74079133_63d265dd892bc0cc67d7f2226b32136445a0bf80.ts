import { Tree } from './tree/tree';
import { Node } from './tree/node';

export class Experiment {
  private result;
  private _trees: Tree[] = [];
  constructor(private R, private m, private n, private stopRule, private mIsConst) {
    let tree;
    let result = [];
    for (let i = 0; i < R; i++) {
      tree = new Tree(m, n, stopRule, mIsConst);
      if (tree.p < n) {
        i--;
        continue;
      }
      this._trees.push(tree);
    }
    this.result = result;
  }
  get trees(): Tree[] {
    return this._trees;
  }
  get roots(): Node[] {
    return this._trees.map((t: Tree) => t.root);
  }
}