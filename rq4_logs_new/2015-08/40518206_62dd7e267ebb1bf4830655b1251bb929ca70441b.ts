// Board.ts by Takumi Fujimoto

/// <reference path="Cell.ts" />
/// <reference path="SandwichChessConstants.ts" />
/// <reference path="Piece.ts" />

class Board {
    cells: Cell[][];

    constructor() {
        this.cells = Board.getCells();
    }
    
    static getCells(): Cell[][] {
        var cells: Cell[][] = [];
        
        for (var row = 0; row < ROWS; row++) {
            cells.push([]);
            
            for (var col = 0; col < COLS; col++) {
                cells[row].push(new Cell(row, col));
            }
        }
        
        return cells;
    }
    
    getPieceInCell(row: number, col: number): Piece {
        return this.cells[row][col].piece;
    }
    
    removePieceFromCell(row: number, col: number): void {
        this.cells[row][col].piece = null;
    }
}