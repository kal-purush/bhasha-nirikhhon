

class Cell {
    hasMine:boolean;
    mineNear:number;
    visible:boolean;
    rowIndex:number;
    colIndex:number;
    isFlaged:boolean;
    constructor(rowIndex, colIndex){
        this.hasMine=false;
        this.mineNear=0;
        this.visible=false;
        this.rowIndex = rowIndex;
        this.colIndex = colIndex;
        this.isFlaged = false;
    }
}

class Grid {
    nbRow:number;
    nbColumn:number;
    grid:Cell[][];
    constructor(row:number,columns:number){
        this.nbRow = row;
        this.nbColumn = columns;
        this.grid = [];
        this.generate();
    }
    private generate() {
        for(var i =0; i< this.nbRow; i++) {
            this.grid[i] = this.grid[i] || [];
            for(var j=0;j< this.nbColumn;j++) {
                this.grid[i][j] = new Cell(i,j);
            }
        }
    }
    addMine(rowIndex:number, colIndex:number){
        var rowStart = rowIndex-1 > 0? rowIndex - 1 : 0;
        var colStart = colIndex-1 > 0 ? colIndex - 1 : 0;
        var cells = this.getNearCell(rowIndex, colIndex);
        while(cells.length > 0) {
            var current = cells.pop();
            current.mineNear++;
        }
        this.grid[rowIndex][colIndex].hasMine = true;
    }
    hasMine(rowIndex:number, colIndex:number) {
        return this.grid[rowIndex][colIndex].hasMine;
    }
    getCell(rowIndex,colIndex): Cell {
        return this.grid[rowIndex][colIndex];
    }
    getNearCell(rowIndex:number, colIndex:number):Cell[] {
        var cells:Cell[] = new Array();
        var minRow = rowIndex-1>0 ? rowIndex-1 : 0;
        var minCol = colIndex - 1 > 0 ? colIndex - 1 : 0;
        var maxRow = rowIndex + 2;
        var maxCol = colIndex + 2;
        maxRow = maxRow < this.nbRow ? maxRow : this.nbRow;
        maxCol = maxCol < this.nbColumn ? maxCol : this.nbColumn;
        for(var i=minRow;i< maxRow;i++) {
            for(var j=minCol;j<maxCol;j++) {
                if(i!=rowIndex || j!=colIndex) {
                    var cell = this.getCell(i,j);
                    if(!cell.visible) {
                        cells.push(cell);
                    }
                }
            }
        }
        return cells;
    }
}

class GridGenerator {
    nbRow:number;
    nbCol:number;
    private nbMine:number;
    constructor(nbRow:number, nbColumn:number, mine:number){
        this.nbRow=nbRow;
        this.nbCol=nbColumn;
        this.nbMine=mine;
    }
    create() {
        var mine = this.nbMine;
        var grid:Grid = new Grid(this.nbRow, this.nbCol);
        while(mine>0) {
            var rowIndex = Math.floor((Math.random() * this.nbRow));
            var columnIndex = Math.floor((Math.random() * this.nbCol));
            if(!grid.hasMine(rowIndex,columnIndex)){
                grid.addMine(rowIndex,columnIndex);
                mine--;
            }
        }
        return grid;
    }
}

class GameCell {
    state:CellState;
    value:number;
    sourceCell:Cell;
    constructor(state:CellState,sourceCell:Cell){
        this.state=state;
        this.sourceCell=sourceCell;
    }
}

enum CellState {HIDDEN,FLAG,VISIBLE,MINE}
enum GameState {WIN,LOST,WIP}

class Deminor {
    private gridGenerator:GridGenerator;
    private grid:Grid;
    private gameStatus:GameState;
    private gameGrid:CellState[][];
    constructor(gridGenerator:GridGenerator) {
        this.gridGenerator = gridGenerator;
        this.gameGrid=[];
        this.gameStatus=GameState.WIP;
    }
    newGame() {
        this.grid = this.gridGenerator.create();
    }
    selectCell(rowIndex:number,columnIndex:number) {
        var cells:Cell[]= new Array();
        var cell:Cell = this.grid.getCell(rowIndex, columnIndex);
        cells.push(cell);
        while(cells.length>0 && this.gameStatus!=GameState.LOST) {
            var currentCell = cells.pop();
            if(!currentCell.hasMine){
                currentCell.visible=true;
                if(currentCell.mineNear == 0) {
                    cells = cells.concat(this.grid.getNearCell(cell.rowIndex, cell.colIndex))
                }
            }
        }
    }
    getCellValue(i,j){
        var value = null;
        var cell = this.grid.getCell(i, j);
        if(cell.visible) {
            value = cell.mineNear;
        } else {
            if(cell.isFlaged) {
                value = -1;
            }
        }
        return value;
    }
    getGameStatus() {
        return this.gameStatus;
    }

    getNbRow() {
        return this.gridGenerator.nbRow;
    }

    getNbCol() {
        return this.gridGenerator.nbCol;
    }
}

var deminor = null;
var display = null;
document.querySelector('#run').addEventListener('click',function() {
   var nbRow = Number((<HTMLInputElement>document.querySelector('#row')).value);
   var nbCol = Number((<HTMLInputElement>document.querySelector('#col')).value);
   var nbMine = Number((<HTMLInputElement>document.querySelector('#mine')).value);
    var gridGenerator = new GridGenerator(nbRow,nbCol,nbMine);
    deminor = new Deminor(gridGenerator);
    deminor.newGame();
});

document.querySelector('#select').addEventListener('click', function () {
    var row = Number((<HTMLInputElement>document.querySelector('#row')).value);
    var col = Number((<HTMLInputElement>document.querySelector('#col')).value);
    deminor.selectCell(row, col);
    displayDeminor()
});

function displayDeminor() {
    var displayDeminor = [];
    for(var i = 0; i<deminor.getNbRow(); i++) {
        displayDeminor[i]=[];
        for(var j=0;j<deminor.getNbCol();j++) {
            displayDeminor[i][j] = deminor.getCellValue(i,j);
        }
    }
    display.table(displayDeminor);
}
