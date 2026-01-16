export class SingleColumnTotal {
  date: string;

  totalPasses: number = 0;
  totalFails: number = 0;
  buildFails: number = 0;
  totalSpecs: number = 0;
  runs: number = 0;
  totalSkips: number = 0;

  constructor (date: string, totals: any) {
    this.date = date;
    this.updateTotal(totals);
  }

  updateTotal (newTotals: any) {
    this.totalSpecs = newTotals.totalSpecs || (newTotals.totalPasses + newTotals.totalFails);
    this.totalPasses += newTotals.totalPasses || 0;
    this.totalFails += newTotals.totalFails || 0;
    this.buildFails += newTotals.status === 'fail' ? this.buildFails += 1 : null;
    this.totalSkips += newTotals.totalSkips || 0;
    this.runs += 1;
  }

  getName () : string {
    return this.date;
  }
}


export class ColumnContainer {
  columns: Array<SingleColumnTotal> = [];

  getColumnIndex(key: string): number {
    let indexOfKey: number = -1;

    this.columns.forEach((column, index) => {
      if(column.getName() === key) {
        indexOfKey = index;
      }
    });

    return indexOfKey;
  }

  addTotals(date: string,  totals: any): void {
    var columnIndex = this.getColumnIndex(date),
        newTotals = {
          totalSpecs: totals.totalSpecs || (totals.totalPasses + totals.totalFails),
          totalPasses: totals.totalPasses || 0,
          totalFails: totals.totalFails || 0,
          status: totals.status || '',
          totalSkips: totals.totalSkips || 0
        };

    if(columnIndex === -1) {
      var column = new SingleColumnTotal(date, newTotals);
      this.columns.push(column);
    } else {
      this.columns[columnIndex].updateTotal(newTotals);
    }
  }

  deleteColumn(key:string): void {
    this.columns.forEach((column, index) => {
      if(column.getName() === key) {
        this.columns.splice(index, 1);
      }
    });
  }

  getColumns(): Array<SingleColumnTotal> {
    return this.columns;
  }
}