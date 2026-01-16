/// <reference path="typings/jquery/jquery.d.ts"/>

// Globals
var WHITE = "white";
var BLACK = "black";
var COLORS = [WHITE, BLACK];
var ROWS = 8;
var COLS = 8;
var SELECTED = "selected";

// Game class
class Game {
    turn: string;
    selectedPiece: Piece;
    pieces: { [keys: string]: Piece[] };
    board: Board;
    static instance: Game;

    init(): void {
        this.board = new Board();

        this.pieces = {};
        this.pieces[WHITE] = Game.getInitialPieces(WHITE);
        this.pieces[BLACK] = Game.getInitialPieces(BLACK);

        this.selectedPiece = null;
        this.turn = WHITE;
    }

    changeTurns(): void {
        this.turn = Game.getOppositeColor(this.turn);
    }

    removeSandwichedPieces(row: number, col: number): void {
        var directions = [{dr:  1, dc:  0},
                          {dr:  0, dc:  1},
                          {dr: -1, dc:  0},
                          {dr:  0, dc: -1}];
        directions.forEach((deltas) => {
            var r = row + deltas.dr;
            var c = col + deltas.dc;
        
            while (Game.withinBounds(r, c)) {
                var piece = this.getPieceInCell(r, c);
                if (piece == null) {
                    break;
                }
            
                if (piece.color == this.turn) {
                    this.removePiecesBetween(r, c, row, col);
                    break;
                }
            
                r += deltas.dr;
                c += deltas.dc;
            }
        });
    }

    removePiecesBetween(row1: number, col1: number, row2: number, col2: number): void {
        if (row1 === row2) {
            var minCol = Math.min(col1, col2) + 1;
            var maxCol = Math.max(col1, col2);
        
            for (var i = minCol; i < maxCol; i++) {
                this.removePiece(this.getPieceInCell(row1, i));
            }
        }
    
        if (col1 === col2) {
            var minRow = Math.min(row1, row2) + 1;
            var maxRow = Math.max(row1, row2);
        
            for (var i = minRow; i < maxRow; i++) {
                this.removePiece(this.getPieceInCell(i, col1));
            }
        }
    }

    makeMove(row: number, col: number): void {
        this.unselectPiece();
        this.selectedPiece.moveTo(row, col, () => this.removeSandwichedPieces(row, col));
        this.changeTurns();
    }

    hasNoPiecesInColBetweenRows(col: number, row1: number, row2: number): boolean {
        var minRow = Math.min(row1, row2);
        var maxRow = Math.max(row1, row2);
    
        for (var i = minRow + 1; i < maxRow; i++) {
            console.log("Checking if the cell is empty at " + i + ", " + col);
            if (this.getPieceInCell(i, col) != null) {
                return false;
            }
        }
        
        return true;
    }

    hasNoPiecesInRowBetweenCols(row: number, col1: number, col2: number): boolean {
        var minCol = Math.min(col1, col2);
        var maxCol = Math.max(col1, col2);
    
        for (var i = minCol + 1; i < maxCol; i++) {
            console.log("Checking if the cell is empty at " + row + ", " + i);
            if (this.getPieceInCell(row, i) != null) {
                return false;
            }
        }
        
        return true;
    }

    isLegitimateMove(row: number, col: number): boolean {
        if (this.selectedPiece === null || this.getPieceInCell(row, col) !== null) {
            return false;
        }
    
        if (row === this.selectedPiece.row) {
            return this.hasNoPiecesInRowBetweenCols(row, col, this.selectedPiece.col);
        }
    
        if (col === this.selectedPiece.col) {
            return this.hasNoPiecesInColBetweenRows(col, row, this.selectedPiece.row);
        }
    
        return false;
    }

    unselectPiece(): void {
        console.log("piece unselected");
        this.selectedPiece.unselect();
        this.selectedPiece = null;
    }

    selectPiece(piece: Piece): void {
        console.log("piece selected");
        this.selectedPiece = piece;
        piece.select();
    }

    getPieceInCell(row: number, col: number): Piece {
        return this.board.cells[row][col].piece;
    }

    removePiece(piece: Piece): void {
        var color = Game.getOppositeColor(this.turn);
        this.pieces[color].splice(this.pieces[color].indexOf(piece), 1);
        
        this.board.removePieceFromCell(piece.row, piece.col);
        piece.hideDomElement();
    }

    placePiece(piece: Piece, row: number, col: number): void {
        this.board.cells[row][col].piece = piece;
    }

    makeMoveIfLegitimate(row: number, col: number): void {
        if (this.isLegitimateMove(row, col)) {
            this.makeMove(row, col);
        } else {
            var pieceInCell = this.getPieceInCell(row, col);
        
            if (this.selectedPiece === pieceInCell) {
                this.unselectPiece();
            } else {
                this.unselectPiece();
            
                if (pieceInCell != null && pieceInCell.color === this.turn) {
                    this.selectPiece(pieceInCell);
                }
            }
        }
    }

    cellClicked(row: number, col: number): void {
        console.log("Cell clicked: (" + row + ", " + col + ")");
    
        if (this.selectedPiece == null) {
            var pieceInCell = this.getPieceInCell(row, col);
            console.log("PieceInCell:" + pieceInCell);
        
            if (pieceInCell != null && pieceInCell.color == this.turn) {
                this.selectPiece(pieceInCell);
            }
        } else {
            this.makeMoveIfLegitimate(row, col);
        }
    }

    static getOppositeColor(color: string): string {
        if (color === WHITE) {
            return BLACK;
        } else {
            return WHITE;
        }
    }

    static getInitialPieces(color: string): Piece[] {
        var pieces = [];

        for (var i = 0; i < COLS; i++) {
            pieces.push(new Piece(i, color));
        }

        return pieces;
    }

    static initDomElementAt(domElement: JQuery, row: number, col: number): void {
        // remove pre-existing bindings
        domElement.off("click");
    
        domElement.click(() => {
            Game.getInstance().cellClicked(row, col);
        });
    }

    static getInstance(): Game {
        if (Game.instance === undefined) {
            Game.instance = new Game();
        }
    
        return Game.instance;
    }
    
    static withinBounds(row: number, col: number): boolean {
        return (0 <= row && row < ROWS) && (0 <= col && col < COLS);
    }
}


// Board class
class Board {
    cells: Cell[][];

    constructor() {
        this.cells = Board.getCells();
    }
    
    removePieceFromCell(row: number, col: number): void {
        this.cells[row][col].piece = null;
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
}


// Piece class
class Piece {
    domElement: JQuery;
    col: number;
    row: number;
    color: string;
    id: number;

    constructor(id: number, color: string) {
        this.id = id;
        this.color = color;
        this.row = Piece.getRow(id, color);
        this.col = Piece.getCol(id);
        this.domElement = $(".piece#" + color + id);
        this.initDomElement();
        Game.getInstance().placePiece(this, this.row, this.col);
    }
    
    initDomElement(): void {
        // remove pre-existing bindings
        this.domElement.off("click");
        this.domElement.click(() => {
            Game.getInstance().cellClicked(this.row, this.col);
        });
        
        this.moveDomElementWithoutAnimationTo(this.row, this.col);
    }
    
    moveTo(row: number, col: number, callback: Function): void {
        Game.getInstance().board.removePieceFromCell(this.row, this.col);
        
        this.row = row;
        this.col = col;
        this.moveDomElementTo(row, col, callback);
        Game.getInstance().placePiece(this, this.row, this.col);
    }
    
    moveDomElementWithoutAnimationTo(row: number, col: number): void {
        var cell = Game.getInstance().board.cells[row][col];
        this.domElement.css(cell.domElement.position());
    }

    moveDomElementTo(row: number, col: number, callback: Function): void {
        var cell = Game.getInstance().board.cells[row][col];
        this.domElement.animate(cell.domElement.position(), 500, callback);
    }
    
    hideDomElement(): void {
        this.domElement.hide();
    }

    static getCol(id: number): number {
        return id;
    }

    static getRow(id: number, color: string): number {
        if (color == WHITE) {
            return 7;
        } else {
            return 0;
        }
    }

    select(): void {
        this.domElement.addClass(SELECTED);
    }
    
    unselect(): void {
        this.domElement.removeClass(SELECTED);
    }
}


// Cell class
class Cell {
    row: number;
    col: number;
    piece: Piece;
    domElement: JQuery;
    
    constructor(row: number, col: number) {
        this.row = row;
        this.col = col;
        this.piece = null;
        this.domElement = $(".cell#r" + row + "c" + col);
        this.initDomElement();
    }
    
    initDomElement(): void {
        Game.initDomElementAt(this.domElement, this.row, this.col);
    }
}