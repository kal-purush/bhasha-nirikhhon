// Adapted from: https://dev.to/codesphere/pathfinding-with-javascript-the-a-algorithm-3jlb
// TERMINOLOGY
// G-Score: Distance travelled from start to current cell
// F-Score: gScore + estimated distance to the goal
// Heuristic = proceeding to a solution by trial and error or by rules that are only loosely defined.

// BEG TYPES
type GridNode = { x: number; y: number };
// END TYPES

let columns = 5;
let rows = 5;
let grid = new Array(columns);

// An array containing unevaluated grid points
let openSet: GridPointClass[] = [];
// An array containing completely evaluated grid points
let closedSet: GridPointClass[] = [];
// The starting grid point
let start: GridPointClass;
// Ending point grid point (goal)
let end: GridPointClass;
// The path of connected Grid Points to the goal
let path: GridPointClass[] = [];

// The heuristic used is Manhattan distance
//for other heuristics visit - https://theory.stanford.edu/~amitp/GameProgramming/Heuristics.html
function heuristic(position0: GridNode, position1: GridNode) {
  let d1 = Math.abs(position1.x - position0.x);
  let d2 = Math.abs(position1.y - position0.y);

  return d1 + d2;
}

class GridPointClass {
  x: number;
  y: number;
  fScore: number;
  gScore: number;
  heuristic: number;
  neighbors: GridPointClass[];
  parent: GridPointClass | undefined;

  constructor(x: number, y: number) {
    this.x = x; // x location (column) of the grid point
    this.y = y; // y location (row) of the grid point
    this.fScore = 0; // total cost function
    this.gScore = 0; // cost function from start to the current grid point
    this.heuristic = 0; // heuristic estimated cost function from current grid point to the goal
    this.neighbors = []; // neighbors of the current grid point
    this.parent = undefined;
  }

  // update neighbors array for a given grid point
  updateNeighbors(grid: GridPointClass[][]) {
    let myX = this.x;
    let myY = this.y;

    if (myX < columns - 1) {
      this.neighbors.push(grid[myX + 1][myY]);
    }

    if (myX > 0) {
      this.neighbors.push(grid[myX - 1][myY]);
    }

    if (myY < rows - 1) {
      this.neighbors.push(grid[myX][myY + 1]);
    }

    if (myY > 0) {
      this.neighbors.push(grid[myX][myY - 1]);
    }
  }
}

// Initializing the grid
function init() {
  // make a 2D array
  for (let index = 0; index < columns; index++) {
    grid[index] = new Array(rows);
  }

  for (let colIndex = 0; colIndex < columns; colIndex++) {
    for (let rowIndex = 0; rowIndex < rows; rowIndex++) {
      grid[colIndex][rowIndex] = new GridPointClass(colIndex, rowIndex);
    }
  }

  for (let colIndex = 0; colIndex < columns; colIndex++) {
    for (let rowIndex = 0; rowIndex < rows; rowIndex++) {
      grid[colIndex][rowIndex].updateNeighbors(grid);
    }
  }

  start = grid[0][0];
  end = grid[columns - 1][rows - 1];

  openSet.push(start);

  console.log(grid);
}

// A star search implementation
// TODO: add params to implement in game
export function search() {
  init();

  while (openSet.length > 0) {
    // assumption that the lowest index is the first one to begin with
    let lowestIndex = 0;
    for (let index = 0; index < openSet.length; index++) {
      if (openSet[index].fScore < openSet[lowestIndex].fScore) {
        lowestIndex = index;
      }
    }
    let currentGridPoint = openSet[lowestIndex];
    if (currentGridPoint === end) {
      let temp = currentGridPoint;
      path.push(temp);
      // while loop below
      while (temp.parent) {
        path.push(temp.parent);
        temp = temp.parent;
      }
      console.log("DONE");

      // return the traced path
      return path.reverse();
    }

    // remove current from openSet
    openSet.splice(lowestIndex, 1);
    // add current to closedSet
    closedSet.push(currentGridPoint);

    let neighbors = currentGridPoint.neighbors;

    for (let index = 0; index < neighbors.length; index++) {
      const singleNeighbor = neighbors[index];

      if (!closedSet.includes(singleNeighbor)) {
        let possibleGscore = currentGridPoint.gScore + 1;

        if (!openSet.includes(singleNeighbor)) {
          openSet.push(singleNeighbor);
        } else if (possibleGscore >= singleNeighbor.gScore) {
          continue;
        }

        singleNeighbor.gScore = possibleGscore;
        singleNeighbor.heuristic = heuristic(singleNeighbor, end);
        singleNeighbor.fScore =
          singleNeighbor.gScore + singleNeighbor.heuristic;
        singleNeighbor.parent = currentGridPoint;
      }
    }
  }

  // no solution by default
  return [];
}