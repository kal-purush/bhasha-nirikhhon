// Copyright 2012 Google Inc.
// All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

/**
  BasicBlock only maintains a vector of in-edges and
  a vector of out-edges.
*/
export class BasicBlock {
    name: number;
    inEdges: Array<BasicBlock> = [];
    outEdges: Array<BasicBlock> = [];

    constructor(name: number) {
        this.name = name;
        BasicBlock.numBasicBlocks++;
    }

    static numBasicBlocks: number = 0;
    static getNumBasicBlocks(): number {
        return BasicBlock.numBasicBlocks;
    }

    toString(): string {
        return `BB$name`;
    }

    getNumPred(): number {
        return this.inEdges.length;
    }

    getNumSucc(): number {
        return this.outEdges.length;
    }

    addInEdge(bb: BasicBlock): void {
        this.inEdges.push(bb);
    }

    addOutEdge(bb: BasicBlock): void {
        this.outEdges.push(bb);
    }
}

/**
  These data structures are stubbed out to make the code below easier
  to review.

  BasicBlockEdge only maintains two pointers to BasicBlocks.
*/
export class BasicBlockEdge {
    from: BasicBlock;
    to: BasicBlock;

    constructor(cfg: CFG, fromName: number, toName: number) {
        this.from = cfg.createNode(fromName);
        this.to = cfg.createNode(toName);

        this.from.addOutEdge(this.to);
        this.to.addInEdge(this.from);

        cfg.addEdge(this);
    }
}

/**
  CFG maintains a list of nodes, plus a start node.
  That's it.
*/
export class CFG {
    basicBlockMap: Map<number, BasicBlock> = new Map<number, BasicBlock>();
    edgeList: Array<BasicBlockEdge> = [];
    startNode: BasicBlock;

    createNode(name: number): BasicBlock {
        var node = this.basicBlockMap.get(name);
        if (node == null) {
            node = new BasicBlock(name);
            this.basicBlockMap.set(name, node);
        }

        if (this.getNumNodes() === 1) {
            this.startNode = node;
        }
        return node;
    }

    addEdge(edge: BasicBlockEdge): void {
        this.edgeList.push(edge);
    }

    getNumNodes(): number {
        return this.basicBlockMap.size;
    }

    getDst(edge: BasicBlockEdge): BasicBlock {
        return edge.to;
    }

    getSrc(edge: BasicBlockEdge): BasicBlock {
        return edge.from;
    }
}