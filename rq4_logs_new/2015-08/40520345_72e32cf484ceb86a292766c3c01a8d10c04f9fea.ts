let fs = require('fs');

let categories = 200;
let series = 10;
let dataPointCount = categories * series;

let sparsity = 0.10;

let isScalar = false;

let lowerBound = -100;
let upperBound = 100;
let range = upperBound - lowerBound;

let precision = 0.001

interface Row extends Array<string> {
}

interface Table extends Array<Row> {
}

function addSparsity(rows: Table): void {
    let targetCount: number = Math.round(sparsity * dataPointCount);
    
    replaceValues(rows, targetCount, "0");
}

function replaceValues(rows: Table, targetCount: number, value: string): void {
    let count = 0;
    while (count < targetCount) {
        let r = Math.round(Math.random() * (categories - 1));
        let c = Math.round(Math.random() * (series - 1));
        
        if (rows[r][c] !== value) {
            rows[r][c] = value;
            count++;
        }
    }
}

function buildDataTable(): Table {
    let rows: Table = [];
    
    for (var c = 0; c < categories; c++) {
      let row: Row = [];
      for (let s = 0; s < series; s++) {
        let val = (Math.random() * range) + lowerBound;
    
        val = Math.round(val / precision) * precision;
    
        row.push(val.toString());
      }
    
      rows.push(row);
    }
    
    return rows;
}

function getSeriesName(row: number): string {
    return "series" + row;
}

function getCategoryName(column: number): string {
    return "category" + column;
}

function toRowStr(row: Row): string {
    return row.join(',');
}

function generateData() {
    // build values
    let table = buildDataTable();
    
    // add sparsity
    addSparsity(table);
    
    // convert to strings & add headers
    let lines: Row = [];
    let header: Row = _.map(_.range(0, table[0].length), (i) => getCategoryName(i));
    lines.push(toRowStr(header));
    for (let r = 0; r < table.length; r++) {
      let row = table[r];
      row.unshift(getSeriesName(r));
      lines.push(toRowStr(row));
    }
    let content = table.join('\r\n');
    
    fs.writeFile("data.csv", content);
}

generateData();