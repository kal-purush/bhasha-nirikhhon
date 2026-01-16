import Cube = require('./Cube');
import Point = require('./Point');

class Box {
  static Size = 5;
  private cubeSequence: Cube[];
  private grid: boolean[][][];

  constructor() {
    this.cubeSequence = [];
    this.grid = [];
    for (var z = 0; z < Box.Size; ++z) {
      this.grid[z] = [];
      for (var x = 0; x < Box.Size; ++x) {
        this.grid[z][x] = [];
        for (var y = 0; y < Box.Size; ++y) {
          this.grid[z][x][y] = false;
        }
      }
    }
  }

  tryAddCube(cube: Cube): boolean {
    if (!Box.checkCubeToBorders(cube)) {
      return false;
    }
    var cubePoints = cube.getAllPoints();
    if (this.isPointsCrossBoxCubes(cubePoints)) {
      return false;
    }

    this.cubeSequence.push(cube);
    this.fillGrid(cubePoints);

    return true;
  }

  private fillGrid(cubePoints: Point[]) {
    for (var i = 0; i < cubePoints.length; ++i) {
      var point = cubePoints[i];
      this.grid[point.z][point.x][point.y] = true;
    }
  }

  removeLastCube(): void {
    this.cubeSequence.pop();
  }

  isPointFilled(point: Point): boolean {
    return this.grid[point.z][point.x][point.y];
  }

  /**
   * return all not filled points on current not filled flow
   */
  getEmptyPoints(): Point[] {
    var emptyPoints: Point[] = [];
    for (var z = 0; z < Box.Size; ++z) {
      if (emptyPoints.length) {
        break;
      }
      for (var x = 0; x < Box.Size; ++x) {
        for (var y = 0; y < Box.Size; ++y) {
          if (!this.grid[z][x][y]) {
            emptyPoints.push(new Point(x, y, z));
          }
        }
      }
    }
    return emptyPoints;
  }

  print() {
    for (var i = 0; i < this.cubeSequence.length; ++i) {
      var cube = this.cubeSequence[i];
      console.log('cube ' + (i + 1));
      cube.print();
      console.log('-----------------------------------');
    }
  }

  /**
   * false if OK
   */
  isPointsCrossBoxCubes(cubePoints: Point[]): boolean {
    for (var i = 0; i < cubePoints.length; ++i) {
      var point = cubePoints[i];
      if (this.isPointFilled(point)) {
        return true;
      }
    }
    return false;
  }

  /**
   * true if OK
   * @param cube
   * @returns {boolean}
   */
  static checkCubeToBorders(cube: Cube): boolean {
    if (!this.checkPointToBorders(cube.startPoint)) {
      return false;
    }
    if (!this.checkPointToBorders(cube.endPoint)) {
      return false;
    }
    return this.checkPointToBorders(cube.ledgePoint);
  }

  static checkPointToBorders(point: Point): boolean {
    if (!this.checkCoordinateToBorders(point.x)) {
      return false;
    }
    if (!this.checkCoordinateToBorders(point.y)) {
      return false;
    }
    return this.checkCoordinateToBorders(point.z);
  }
  static checkCoordinateToBorders(coordinate: number): boolean {
    return (coordinate >= 0) && (coordinate < Box.Size);
  }
}

export = Box;