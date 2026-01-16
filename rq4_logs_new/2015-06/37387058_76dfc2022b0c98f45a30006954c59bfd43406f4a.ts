
function get(what) {
    return document.getElementById(what);
}

class Game {

    static canvas: HTMLCanvasElement;
    static canvasSize;
    static context: CanvasRenderingContext2D;
    static board: Board;
    static mode: string = 'AI';
    static over: boolean = false;

    static init(): void {
        Game.canvas = (<HTMLCanvasElement> get('gameCanvas'));
        Game.canvasSize = { w: Game.canvas.width, h: Game.canvas.height };
        Game.context = Game.canvas.getContext('2d');

        Game.board = new Board();

        Game.render();
    }

    static render(): void {
        Game.context.fillStyle = "white";
        Game.context.fillRect(0, 0, Game.canvasSize.w, Game.canvasSize.h);

        Game.board.render();
    }

}

class Board {

    static tileSize = 56;
    static offset;

    width: number;
    height: number;
    tiles: string[][];
    winStates: string[];
    hoverTile = { x: null, y: null, bx: null, by: null };
    playerTurn: string = 'X';
    playableArea: number = 10; // 0=anywhere, 1=only first cell, 2=only second cell, etc.

    constructor() {
        this.clear();
    }

    hover(xx: number, yy: number): void {
        if (Game.over) return;
        var bx = Math.floor((xx - 10) / (Board.tileSize * 3 + 10)),
            by = Math.floor((yy - 10) / (Board.tileSize * 3 + 10));
        bx = this.clamp(bx, 0, 2);
        by = this.clamp(by, 0, 2);

        var x = Math.floor((xx - 10 - (Board.tileSize * 3 + 10) * bx) / (Board.tileSize)),
            y = Math.floor((yy - 10 - (Board.tileSize * 3 + 10) * by) / (Board.tileSize));
        x = this.clamp(x, 0, 2);
        y = this.clamp(y, 0, 2);

        if (this.tiles[bx + by * 3][x + y * 3] === '' && !this.winStates[bx + by * 3]) {
            this.hoverTile.bx = bx;
            this.hoverTile.by = by;
            this.hoverTile.x = x;
            this.hoverTile.y = y;
        } else {
            this.hoverTile.x = null;
            this.hoverTile.y = null;
            this.hoverTile.bx = null;
            this.hoverTile.by = null;
        }
    }

    click(event: MouseEvent, xx: number, yy: number): void {
        var type = clickType(event);
        if ((type === 'right' || type === 'left') && this.hoverTile.x !== null && this.hoverTile.y !== null) {
            var bxy = this.hoverTile.bx + this.hoverTile.by * 3,
                xy = this.hoverTile.x + this.hoverTile.y * 3;

            if (bxy === this.playableArea || this.playableArea === 10) {
                this.setTile(bxy, xy, this.playerTurn);

                this.playableArea = xy;
                if (this.winStates[this.playableArea] !== '') this.playableArea = 10;

                this.hoverTile.x = null;
                this.hoverTile.y = null;
                this.hoverTile.bx = null;
                this.hoverTile.by = null;

                this.checkForWinners();

                if (Game.mode === 'AI') {
                    this.playableArea = AI.makeMove(this, this.playableArea);
                    if (this.winStates[this.playableArea] !== '') this.playableArea = 10;
                } else {
                    this.playerTurn = this.playerTurn === 'X' ? 'O' : 'X';
                }
            }
        }

    }

    clamp(n: number, min: number, max: number): number {
        return n >= min && n <= max ? n : null;
    }

    checkGameWinners(): void {
        var gamewinner = this.getBoardWinState(this.winStates);
        if (gamewinner && gamewinner !== '') {
            Game.over = true;
            console.log("game over! winner: ", gamewinner);
        }
    }

    checkForWinners(): void {
        for (var b = 0; b < this.tiles.length; b++) {
            this.winStates[b] = this.getBoardWinState(this.tiles[b]);
        }
        this.checkGameWinners();
    }

    getBoardWinState(tiles: Array<string>): string {
        if (tiles[0] && tiles[0] === tiles[1] && tiles[1] === tiles[2]) return tiles[0];
        if (tiles[3] && tiles[3] === tiles[4] && tiles[4] === tiles[5]) return tiles[3];
        if (tiles[6] && tiles[6] === tiles[7] && tiles[7] === tiles[8]) return tiles[6];

        if (tiles[0] && tiles[0] === tiles[3] && tiles[3] === tiles[6]) return tiles[0];
        if (tiles[1] && tiles[1] === tiles[4] && tiles[4] === tiles[7]) return tiles[1];
        if (tiles[2] && tiles[2] === tiles[5] && tiles[5] === tiles[8]) return tiles[2];

        if (tiles[0] && tiles[0] === tiles[4] && tiles[4] === tiles[8]) return tiles[0];
        if (tiles[2] && tiles[2] === tiles[4] && tiles[4] === tiles[6]) return tiles[2];

        return '';
    }

    render(): void {
        // Large grid
        Game.context.strokeStyle = "#222";
        Game.context.lineWidth = 3;
        this.drawGrid(5, 5, Board.tileSize * 3 + 10);

        // playable area
        if (this.playableArea === 10) {
            Game.context.strokeStyle = "#6D7";
            Game.context.lineWidth = 20;
            Game.context.lineJoin = 'round';
            Game.context.strokeRect(0, 0, (Board.tileSize * 3 + 10) * 3 + 10, (Board.tileSize * 3 + 10) * 3 + 10);
        } else {
            Game.context.strokeStyle = "#6D7";
            Game.context.lineWidth = 10;
            Game.context.lineJoin = 'round';
            Game.context.strokeRect((this.playableArea % 3) * (Board.tileSize * 3 + 10) + 5, Math.floor(this.playableArea / 3) * (Board.tileSize * 3 + 10) + 5, (Board.tileSize * 3 + 10), (Board.tileSize * 3 + 10));
        }

        for (var b = 0; b < this.tiles.length; b++) {
            var boardOffset = {
                x: (b % 3) * (3 * Board.tileSize + 10) + 10,
                y: Math.floor(b / 3) * (3 * Board.tileSize + 10) + 10
            };
            if (this.winStates[b] && this.winStates[b] !== '') {
                // win state background
                Game.context.fillStyle = this.winStates[b] === 'X' ? '#6C6' : '#E56';
                Game.context.fillRect(boardOffset.x, boardOffset.y, Board.tileSize * 3, Board.tileSize * 3);
                Game.context.font = '210px SourceSansPro-Regular';
                Game.context.fillStyle = this.winStates[b] === 'X' ? '#7F7' : '#F67';
                Game.context.fillText(this.winStates[b], boardOffset.x + (this.winStates[b] === 'X' ? 30 : 16), boardOffset.y + 153);
            }
            Game.context.fillStyle = "black";
            Game.context.font = '60px SourceSansPro-ExtraLight';
            for (var x = 0; x < this.tiles[b].length; x++) {
                var innerOffset = {
                    x: (x % 3) * Board.tileSize + boardOffset.x + (this.tiles[b][x] === 'X' ? 15 : 9),
                    y: Math.floor(x / 3) * Board.tileSize + boardOffset.y
                };
                // individual tiles
                Game.context.fillText(this.tiles[b][x], innerOffset.x - 1, innerOffset.y + 49);
            }
        }

        // Small grids
        Game.context.strokeStyle = "#777";
        Game.context.lineWidth = 2;
        for (var i = 0; i < 9; i++) {
            var offset = {
                x: (i % 3) * (3 * Board.tileSize + 10) + 10,
                y: Math.floor(i / 3) * (3 * Board.tileSize + 10) + 10
            };
            this.drawGrid(offset.x, offset.y, Board.tileSize);
        }

        if (this.hoverTile.x !== null && this.hoverTile.y !== null) {
            Game.context.fillStyle = "rgba(130, 100, 160, 0.2)";
            Game.context.fillRect(this.hoverTile.x * Board.tileSize + 10 + (Board.tileSize * 3 + 10) * this.hoverTile.bx,
                this.hoverTile.y * Board.tileSize + 10 + (Board.tileSize * 3 + 10) * this.hoverTile.by, Board.tileSize, Board.tileSize);
            Game.context.fillStyle = 'white';
            Game.context.fillText(this.playerTurn, this.hoverTile.x * Board.tileSize + 10 + (Board.tileSize * 3 + 10) * this.hoverTile.bx + (this.playerTurn === 'X' ? 15 : 9),
                this.hoverTile.y * Board.tileSize + 10 + (Board.tileSize * 3 + 10) * this.hoverTile.by + 49);
        }
    }

    /* x, y: starting corner of grid, x+3w, y+3h: ending corner of grid */
    drawGrid(x: number, y: number, colw: number): void {
        Game.context.beginPath();
        Game.context.moveTo(x, y + colw);
        Game.context.lineTo(x + 3 * colw, y + colw);
        Game.context.stroke();

        Game.context.moveTo(x, y + 2 * colw);
        Game.context.lineTo(x + 3 * colw, y + 2 * colw);
        Game.context.stroke();

        Game.context.moveTo(x + colw, y);
        Game.context.lineTo(x + colw, y + 3 * colw);
        Game.context.stroke();

        Game.context.moveTo(x + 2 * colw, y);
        Game.context.lineTo(x + 2 * colw, y + 3 * colw  );
        Game.context.stroke();
        Game.context.closePath();
    }

    setTile(board: number, position: number, player: string): boolean {
        var tile = this.tiles[board][position];
        if (tile !== 'X' && tile !== 'O') {
            this.tiles[board][position] = player;
            return true;
        }
        return false;
    }

    clear(): void {
        this.winStates = new Array<string>(9);
        this.tiles = new Array<Array<string>>(9);
        for (var i = 0; i < this.tiles.length; i++) {
            this.tiles[i] = new Array<string>(9);
            for (var j = 0; j < this.tiles[i].length; j++) {
                this.tiles[i][j] = '';
            }
        }

        this.playableArea = 10;

        Game.over = false;

        // this.playerTurn = Math.floor(Math.random() * 2) === 1 ? 'X' : 'O';
        this.playerTurn = 'X';

        Game.mode = (<HTMLInputElement> get('myonoffswitch')).checked ? 'AI' : 'PLAYER';

        this.hoverTile.x = null;
        this.hoverTile.y = null;
        this.hoverTile.bx = null;
        this.hoverTile.by = null;
    }
}

// TODO implement popup for new game

class AI {

    static makeMove(board: Board, playableArea: number): number {
        var b: number, p: number;

        do {
            if (playableArea === 10) b = Math.floor(Math.random() * 9);
            else b = playableArea;
            p = Math.floor(Math.random() * 9);
        } while (!board.setTile(b, p, 'O'));

        board.tiles[b][p] = 'O';

        board.checkForWinners();

        return p; // return the playable area the player has to play in
    }

}

function newGame(): void {
    Game.board.clear();
    Game.render();
}

function clickType(event): string {
    if (event.which === 3 || event.button === 2) return "right";
    else if (event.which === 1 || event.button === 0) return "left";
    else if (event.which === 2 || event.button === 1) return "middle";
}

function boardHover(event: MouseEvent): void {
    if (Game.board) {
        Game.board.hover(event.offsetX, event.offsetY);
        Game.render();
    }
}

function boardClick(event: MouseEvent): void {
    Game.board.click(event, event.offsetX, event.offsetY);
    Game.render();
}

window.onload = function(event) {
    Game.init();
}