import { Card } from './card.model';

export class FinalPathTree {
    top: FinalPath;
    children: FinalPathTree[];

    constructor(finalPath: FinalPath) {
        this.top = finalPath;
        this.children = [];
    }
}

export class FinalPathBinaryTree {

    data: FinalPath;
    left: FinalPathBinaryTree;
    right: FinalPathBinaryTree;
    size: number;

    constructor(data: FinalPath) {
        this.data = data;
        this.left = null;
        this.right = null;
        this.size = 1;
    }

    add(finalPath: FinalPath) {
        if (!this.left) {
            this.left = new FinalPathBinaryTree(finalPath);
        } else if (!this.right) {
            this.right = new FinalPathBinaryTree(finalPath);
        } else if (this.leftFull()) {
            this.right.add(finalPath);
        } else {
            this.left.add(finalPath);
        }
        this.size++;
    }

    getLast(current: FinalPathBinaryTree) {
        if (!current.left) {
            return current;
        } else if (!current.right) {
            return current;
        } else if (current.leftFull()) {
            current = this.getLast(current.right);
        } else {
            current = this.getLast(current.left);
        }
        return current;
    }

    leftFull(): boolean {
        let leafs = 1;
        let used: number;
        while (leafs <= (this.size + 1)) {
            leafs *= 2;
        }
        leafs = Math.floor(leafs / 2);
        used = (this.size + 1) % leafs;
        if (used >= Math.floor(leafs / 2)) {
            return true;
        }
        else {
            return false;
        }
    }
}

export class FinalPath {
    rank4Pair: Card[];
    rank5: Card;
    merged: Card;
    goal: Card;

    constructor() {

    }
}