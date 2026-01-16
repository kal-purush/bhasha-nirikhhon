import {Graph, Node} from "./Graph";
import _ = require('lodash');
import {Backtrack} from "./Backtrack";
import {notPresent} from "./Util";
import {last} from "./Util";
import {Stack} from "./Stack";
/**
 * Point here is to set up a grid of cells that exist between the edges
 * of a grid-shaped graph. E.g., where C is a cell, N is a node, line is
 * an edge:
 *
 * N-N-N
 * |C|C|
 * N-N-N
 * |C|C|
 * N-N-N
 *
 * This'll let us set up closed regions of the grid defined by connected
 * nodes.
 */
export class Grid {
    private x: number;
    private y: number;
    private cellX: number;
    private cellY: number;
    private graph: Graph<number>;
    private regionGraph: Graph<number>;

    constructor(x: number, y: number) {
        this.x = x;
        this.y = y;
        this.cellX = x-1;
        this.cellY = y-1;

        this.graph = createGridGraph(x, y);
        this.regionGraph = createGridGraph(x, y, true);
    }

    Graph() { return this.graph; }

    DeleteNode(index: number) {
        this.graph.DeleteNodeAt(index);
        this.regionGraph.DeleteNodeAt(index);
        return this;
    }

    // Don't delete the edge from the shadow graph, unless we're completely disconnecting
    // the node.
    DeleteEdgeFrom(fromIndex: number, toIndex: number) {
        var graph = this.graph;
        this.graph.DeleteEdgeFromIndex(fromIndex, toIndex);

        if(graph.NodeAt(fromIndex).Nodes().length === 0) this.regionGraph.DeleteNodeAt(fromIndex);
        if(graph.NodeAt(toIndex).Nodes().length === 0) this.regionGraph.DeleteNodeAt(toIndex);

        return this;
    }

    NodesBoundingCell(cellX: number, cellY: number) {
        var bottomLeft = cellY * this.x + cellX;
        var topLeft = (cellY + 1) * this.x + cellX;

        return [
            bottomLeft,
            bottomLeft + 1,
            topLeft,
            topLeft + 1
        ]
    }

    CellExists(cellX: number, cellY: number) {
        var boundingNodes = this.NodesBoundingCell(cellX, cellY);
        var len = boundingNodes.length;

        for(var i = 0; i < len; i++) {
            if(!this.graph.NodeIsConnected(boundingNodes[i])) {
                return false;
            }
        }

        return true;
    }

    CellsConnected(cellAX: number, cellAY: number, cellBX: number, cellBY: number, solution: Node<number>[]) {
        // stupid bounds checking
        if(cellAY >= this.cellY || cellAY < 0 || cellAX >= this.cellX || cellAX < 0) return false;
        if(cellBY >= this.cellY || cellBY < 0 || cellBX >= this.cellX || cellBX < 0) return false;

        // naive connectivity
        let xDiff = cellBX - cellAX;
        let yDiff = cellBY - cellAY;

        // No diagonals
        if(xDiff && yDiff) return false;
        if(!xDiff && !yDiff) return false;

        let firstIndex: number;
        let secondIndex: number;
        if(xDiff) {
            let nodeX = cellAX + (xDiff === 1 ? 1 : 0);
            firstIndex = nodeX + cellAY * this.x;
            secondIndex = firstIndex + this.y;
        } else {
            let nodeY = cellAY + (yDiff === 1 ? 1 : 0);
            firstIndex = cellAX + nodeY * this.x;
            secondIndex = firstIndex + 1;
        }

        //console.log('indices: ' + firstIndex + ', ' + secondIndex);

        // No revisiting, so the first time we find the index, we can return the answer immediately.
        // So if there's a solution edge between the cells, return false.
        for(let i = 0; i < solution.length - 1; i++) {
            var node = solution[i];
            var index = node.Index();

            if(firstIndex === index) {
                return solution[i+1].Index() !== secondIndex;
            }

            if(secondIndex === index) {
                return solution[i+1].Index() !== firstIndex;
            }
        }

        return true;
    }

    DetermineAllRegions(solution : Node<number>[]) {
        let len = this.cellX * this.cellY;
        let regionIndexes = new Array<number>(len);
        let regionNumber = 1;

        for(let i = 0; i < len; i++) {
            if(regionIndexes[i]) continue;

            this.FloodFill(i % this.cellX, Math.floor(i / this.cellX), solution, regionIndexes, regionNumber);
            regionNumber++;
        }

        return regionIndexes;
    }

    IsEdgeNode(index: number) {
        return this.regionGraph.NodeAt(index).Nodes().length < 8;
    }

    FloodFill(cellX: number, cellY: number, solution: Node<number>[], regionCells: number[], regionNumber: number = 1) {
        const stack = new Stack<[number, number]>(this.cellY + 1);
        //const regionCells: boolean[] = new Array<boolean>(this.cellX * this.cellY);

        stack.push([cellX, cellY]);

        const connectedUp = (x, y) =>
            y < this.cellY &&
            !regionCells[x + (y+1)*this.cellX] &&
            this.CellsConnected(x, y, x, y + 1, solution);

        const connectedDown = (x, y) =>
            y > 0 &&
            !regionCells[x + (y-1)*this.cellX] &&
            this.CellsConnected(x, y, x, y - 1, solution);

        const connectedRight = (x, y) =>
            x < this.cellX &&
            !regionCells[x + 1 + y*this.cellX] &&
            this.CellsConnected(x, y, x + 1, y, solution);

        const connectedLeft = (x, y) =>
            x > 0 &&
            !regionCells[x - 1 + (y+1)*this.cellX] &&
            this.CellsConnected(x, y, x - 1, y, solution);

        while(stack.Size()) {
            let [x1, y] = stack.pop();

            while(x1 >= 0 && connectedLeft(x1, y)) x1--;

            let spanAbove: boolean;
            let spanBelow: boolean;
            spanAbove = spanBelow = false;

            //console.log('entering inner with ' + x1 + ', ' + y);
            while(x1 < this.cellX) {
                regionCells[x1 + this.cellX * y] = regionNumber;
                const up = connectedUp(x1, y);
                if(!spanAbove && up) {
                    stack.push([x1, y + 1]);
                    //console.log('span above at ' + x1 + ', ' + y);
                    spanAbove = true;
                }
                else if(spanAbove && !up) spanAbove = false;

                const down = connectedDown(x1, y);
                if(!spanBelow && down) {
                    stack.push([x1, y - 1]);
                    //console.log('span below at ' + x1 + ', ' + y);
                    spanBelow = true;
                }
                else if(spanBelow && !down) spanBelow = false;

                if(!connectedRight(x1, y)) break;
                x1++;
                //console.log('continuing at ' + x1 + ', ' + y);
            }
        }

        return regionCells;
    }
}

function getEdges(i: number, x: number, y: number, eightDir: boolean, edges: [number, number][]) {
    var row = Math.floor(i / x);
    var col = i % x;

    var up = row < y-1;
    var right = col < x-1;
    var left = row > 0;

    if(up) {
        edges.push([i, i+x]);
        if(eightDir && left) edges.push([i, i+x - 1]);
        if(eightDir && right) edges.push([i, i+x + 1])
    }
    if(right) edges.push([i, i+1]);

    return edges;
}

export function createGridGraph(x: number, y: number, eightDir: boolean = false) {
    var n = x * y;
    var indices = _.range(n);

    var edges : [number, number][] = [];
    _.each(indices, i => getEdges(i, x, y, eightDir, edges));

    return new Graph(indices, edges);
}