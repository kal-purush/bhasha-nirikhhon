declare module 'ascii-table' {
  type Name = string | object
  type Cell = string | number

  class AsciiTable {
    constructor(name?: Name, options?: object)
    
    clear(name: Name): AsciiTable
    reset(name: Name): AsciiTable
    setBorder(edge: string, fill: string, top: string, bottom: string): AsciiTable
    removeBorder(): AsciiTable
    setAlign(idx: number, dir: number): AsciiTable
    setTitle(title: string): AsciiTable
    getTitle(): string
    setTitleAlign(dir: number): AsciiTable
    sort(method: Function): AsciiTable
    sortColumn(idx: number, method: Function): AsciiTable
    setHeading(row: Cell[]): AsciiTable
    setHeading(...row: Cell[]): AsciiTable
    getHeading(): Cell[]
    setHeadingAlign(dir: number): AsciiTable
    addRow(row: Cell[]): AsciiTable
    addRow(...row: Cell[]): AsciiTable
    getRows(): Cell[]

    addRowMatrix(rows: Cell[][]): AsciiTable
    addData(data: Cell[], rowCallback: Function, asMatrix: boolean): AsciiTable
    clearRows(): AsciiTable
    setJustify(val: boolean): AsciiTable

    toJSON(): {
      title: ReturnType<typeof AsciiTable.prototype.getTitle>
      heading: ReturnType<typeof AsciiTable.prototype.getHeading>
      rows: ReturnType<typeof AsciiTable.prototype.getRows>
    }

    parse(obj: object): AsciiTable
    fromJSON(obj: object): AsciiTable

    render(): string
    valueOf(): string
    toString(): string
  }

  export default AsciiTable
}