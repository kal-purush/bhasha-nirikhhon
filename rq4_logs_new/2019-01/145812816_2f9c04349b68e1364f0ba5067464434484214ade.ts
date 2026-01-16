import {action, computed, observable} from "mobx";

class GameStore {
  public static readonly INITIAL_FIELD = [
    1, 2, 3, 4, 5, 6, 7, 8, 9,
    1, 1, 1, 2, 1, 3, 1, 4, 1,
    5, 1, 6, 1, 7, 1, 8, 1, 9,
  ];

  public readonly rowSize: number = 9

  @observable
  public history: number[][] = [[...GameStore.INITIAL_FIELD]]

  @observable
  public positionInHistory: number = 0

  @observable
  public previousSelectedNumberIndex?: number

  @observable
  public crossoutsMade: number = 0

  @computed
  public get previousSelectedNumber() {
    if (this.previousSelectedNumberIndex !== undefined &&
        this.previousSelectedNumberIndex >= 0 &&
        this.previousSelectedNumberIndex < this.cells.length) {
      return this.cells[this.previousSelectedNumberIndex];
    }
    return undefined;
  }

  @computed
  public get previousSelectedNumberRow() {
    return (
      this.previousSelectedNumberIndex &&
      Math.floor(this.previousSelectedNumberIndex / this.rowSize)
    );
  }

  @computed
  public get previousSelectedNumberCol() {
    return (
      this.previousSelectedNumberIndex &&
      this.previousSelectedNumberIndex % this.rowSize
    );
  }

  public numbersMatch(a?: number, b?: number) {
    return a && b && a + b === this.rowSize + 1 || a === b;
  }

  @action
  public handleNumberClick(row: number, col: number) {
    const clickedNumberIndex = row * this.rowSize + col;
    const clickedNumber = this.cells[clickedNumberIndex];

    if (clickedNumber === 0) {
      return;
    }

    if (!clickedNumber || clickedNumber < 0 || clickedNumber >= 10) {
      throw Error(`Unexpected number ${clickedNumber}`);
    }

    if (this.previousSelectedNumberIndex === undefined) {
      this.previousSelectedNumberIndex = clickedNumberIndex;
      return;
    }

    if (this.previousSelectedNumberIndex === clickedNumberIndex) {
      this.previousSelectedNumberIndex = undefined;
    } else if (this.areNeighbors(clickedNumberIndex, this.previousSelectedNumberIndex)) {
      if (this.numbersMatch(clickedNumber, this.previousSelectedNumber)) {
        this.crossOut(this.previousSelectedNumberIndex, clickedNumberIndex);
      } else {
        this.previousSelectedNumberIndex = clickedNumberIndex;
      }
    } else {
      this.previousSelectedNumberIndex = clickedNumberIndex;
    }
  }

  @action
  private crossOut(index1: number, index2: number) {
    const numbers = this.cells.slice();
    numbers[index1] = 0;
    numbers[index2] = 0;
    this.previousSelectedNumberIndex = undefined;

    this.history = this.history.slice(0, this.positionInHistory + 1).concat([numbers]);
    this.positionInHistory++;
    this.removeZeroRows();
    this.crossoutsMade++;
  }

  @computed
  public get canUndo() {
    return this.positionInHistory > 0;
  }

  @action
  public undo() {
    this.positionInHistory--;
    this.crossoutsMade--;
  }

  @computed
  public get canRedo() {
    return this.positionInHistory < this.history.length - 1;
  }

  @action
  public redo() {
    this.positionInHistory++;
    this.crossoutsMade++;
  }

  private removeZeroRows() {
    const rowsToRemove: number[] = [];
    for (let i = 0; i < this.cells.length - this.rowSize; i += this.rowSize) {
      const row = this.cells.slice(i, i + this.rowSize);
      if (row.every((n) => n === 0)) {
        rowsToRemove.push(i);
      }
    }
    rowsToRemove.reverse().forEach((i) => {
      this.cells.splice(i, this.rowSize);
    });
  }

  @action
  public nextLevel() {
    const numbers = this.cells.slice();
    const positiveNumbers = numbers.filter((n) => n > 0);
    numbers.push(...positiveNumbers);
    this.history = [numbers];
    this.positionInHistory = 0;
  }

  @action
  public reset() {
    this.history = [[...GameStore.INITIAL_FIELD]];
    this.positionInHistory = 0;
    this.crossoutsMade = 0;
  }

  private areNeighbors(index1: number, index2: number) {
    return this.neighbors(index1).indexOf(index2) >= 0;
  }

  private neighbors(index: number) {
    const neighbors: number[] = [];
    [-this.rowSize, -1, +1, +this.rowSize].forEach((offset) => {
      let i = index;
      while (true) {
        i += offset;
        if (i < 0 || i >= this.cells.length) {
          break;
        }
        if (this.cells[i] !== 0) {
          neighbors.push(i);
          break;
        }
      }
    });
    return neighbors;
  }

  @computed
  public get cells() {
    return this.history[this.positionInHistory];
  }

  @computed
  public get rows() {
    const result = [];
    for (let sliceStart = 0; sliceStart < this.cells.length; sliceStart += this.rowSize) {
      const sliceEnd = Math.min(sliceStart + this.rowSize, this.cells.length);
      result.push(this.cells.slice(sliceStart, sliceEnd));
    }
    return result;
  }
}

export default GameStore