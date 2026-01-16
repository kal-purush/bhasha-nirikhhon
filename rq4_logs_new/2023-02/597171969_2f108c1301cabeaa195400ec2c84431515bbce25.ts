import { Boundary } from "./boundary-class";

export class GridPointClass {
  xGrid: number;
  yGrid: number;
  xPixel: number;
  yPixel: number;
  fScore: number;
  gScore: number;
  heuristic: number;
  neighbors: GridPointClass[];
  parent: GridPointClass | undefined;

  constructor({ xGrid: x, yGrid: y }: { xGrid: number; yGrid: number }) {
    this.xGrid = x; // x location (column) of the grid point
    this.yGrid = y; // y location (row) of the grid point
    this.xPixel = this.xGrid * Boundary.cellWidth;
    this.yPixel = this.yGrid * Boundary.cellHeight;
    this.fScore = 0; // total cost function
    this.gScore = 0; // cost function from start to the current grid point
    this.heuristic = 0; // heuristic estimated cost function from current grid point to the goal
    this.neighbors = []; // neighbors of the current grid point
    this.parent = undefined;
  }

  // update neighbors array for a given grid point
  updateNeighbors(grid: GridPointClass[][]) {
    let myX = this.xGrid;
    let myY = this.yGrid;
    let columnsLen = grid[0].length;
    let rowsLen = grid.length;

    if (myX < columnsLen - 1) {
      this.neighbors.push(grid[myX + 1][myY]);
    }

    if (myX > 0) {
      this.neighbors.push(grid[myX - 1][myY]);
    }

    if (myY < rowsLen - 1) {
      this.neighbors.push(grid[myX][myY + 1]);
    }

    if (myY > 0) {
      this.neighbors.push(grid[myX][myY - 1]);
    }
  }
}