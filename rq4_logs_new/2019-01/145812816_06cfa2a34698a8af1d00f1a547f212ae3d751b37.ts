export default class CellIndex {
  private readonly _row: number

  private readonly _col: number

  public constructor(row: number, col: number) {
    this._row = row
    this._col = col
  }

  public get row(): number {
    return this._row
  }

  public get col(): number {
    return this._col
  }

  public static fromSerial(serial: number, rowSize: number): CellIndex {
    const row = Math.floor(serial / rowSize)
    const col = serial - row * rowSize
    return new CellIndex(row, col)
  }

  public serial(rowSize: number): number {
    return this.row * rowSize + this.col
  }
}