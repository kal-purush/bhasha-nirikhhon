import Row from './Row';
import Fail from './Fail';

export default class Board {
  private rows: Array<Row> = [];
  private fails: Array<Fail> = [];

  public setRows(rows: Array<Row>) {
    this.rows = rows;
  }

  public getRows(): Array<Row> {
    return this.rows;
  }

  public setFails(fails: number) {
    this.fails = Array.apply({}, Array(fails)).map(() => new Fail());
  }

  public getFails(): Array<Fail> {
    return this.fails;
  }

  public markNumber(rowIndex: number, numberIndex: number): string {
    if (this.isFinished()) {
      return;
    }
    return this.rows[rowIndex].markNumber(numberIndex);
  }

  public closeRow(rowIndex: number) {
    if (this.isFinished()) {
      return;
    }
    this.rows[rowIndex].closeRow();
  }

  public failFail(failIndex: number) {
    if (this.isFinished()) {
      return;
    }
    this.fails[failIndex].failFail();
  }

  public getColorPoints(color: string): number {
    const marked = this.getRows().map((row: Row) => row.countNumbersMarkedByColor(color))
      .reduce((colorPointsA: number, colorPointsB: number) => colorPointsA + colorPointsB, 0);
    let colorPoints = 0;
    for (let increase = 1; increase <= marked; ++increase) {
      colorPoints += increase;
    }
    return colorPoints;
  }

  public getFailPoints(): number {
    return this.countFailsFailed() * -5;
  }

  public getPoints(): number {
    let points = 0;
    for (const color of ['red', 'yellow', 'green', 'blue']) {
      points += this.getColorPoints(color);
    }
    points += this.getFailPoints();
    return points;
  }

  public isOpen(): boolean {
    return this.countRowsClosed() < 2 && this.countFailsFailed() < 4;
  }

  public isFinished(): boolean {
    return !this.isOpen();
  }

  private countRowsClosed(): number {
    return this.rows.filter((row: Row) => row.isRowClosed()).length;
  }

  private countFailsFailed(): number {
    return this.fails.filter((fail: Fail) => fail.isFailFailed()).length;
  }
}