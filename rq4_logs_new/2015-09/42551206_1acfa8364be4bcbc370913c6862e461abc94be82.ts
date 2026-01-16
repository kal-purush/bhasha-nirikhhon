class GameOfLife {
    private canvas: HTMLCanvasElement;
    private ctx: CanvasRenderingContext2D;
    private board: boolean[][];
    private boardWidth: number;
    private boardHeight: number;
    private directionOffsets: Offset[] = [
        new Offset(-1, -1), new Offset(0, -1), new Offset(1, -1),
        new Offset(-1, 0),                    new Offset(1, 0),
        new Offset(-1, 1),  new Offset(0, +1), new Offset(1, 1),
    ];
    private nextFrameFunction: Function;

    constructor() {
        this.canvas = <HTMLCanvasElement>document.getElementById('board');
        this.ctx = <CanvasRenderingContext2D>this.canvas.getContext('2d');
        this.boardWidth = this.canvas.width;
        this.boardHeight = this.canvas.height;
        this.board = GameOfLife.initBoard(this.boardWidth, this.boardHeight);
    }

    public start() {
        this.seedBoard();
        this.nextFrame();
    }

    private update() {
        var nextBoard: boolean[][] = GameOfLife.initBoard(this.boardWidth, this.boardHeight);

        for (var y = 0; y < this.board.length; y++) {
            for (var x = 0; x < this.board[y].length; x++) {
                var aliveCount = this.getCountOfAliveNeighbors(x, y);

                var cellCurrentState: boolean = this.board[y][x];
                var cellFate: boolean = false;

                if (cellCurrentState === true && (aliveCount == 2 || aliveCount == 3)) {
                    cellFate = true;
                }
                else if (cellCurrentState === false && aliveCount == 3) {
                    cellFate = true;
                }

                nextBoard[y][x] = cellFate;
            }
        }

        this.board = nextBoard;
    }

    private getCountOfAliveNeighbors(x: number, y: number): number {
        var aliveCount = 0;

        for (var i = 0; i < this.directionOffsets.length; i++) {
            var offset = this.directionOffsets[i];
            var neighborX: number = x + offset.x;
            var neighborY: number = y + offset.y;

            if (neighborX >= 0 && neighborX < this.boardWidth &&
                neighborY >= 0 && neighborY < this.boardHeight &&
                this.board[neighborY][neighborX] === true) {
                aliveCount++;
            }
        }

        return aliveCount;
    }

    private render() {
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
        for (var y = 0; y < this.canvas.height; y++) {
            for (var x = 0; x < this.canvas.width; x++) {
                if (this.board[y][x]) {
                    this.ctx.fillRect(x, y, 1, 1);
                }
            }
        }
    }

    private nextFrame() {
        this.render();
        this.update();
        requestAnimationFrame(this.nextFrame.bind(this));
    }

    private static initBoard(width: number, height: number): boolean[][] {

        var state: boolean[][] = [];

        for (var y = 0; y < height; y++) {
            state.push([]);
            for (var x = 0; x < width; x++) {
                state[y][x] = false;
            }
        }

        return state;
    }

    private seedBoard() {
        var spaceFiller: String[] = [
            "0000000000000000000011100011100000000000000000000",
            "0000000000000000000100100010010000000000000000000",
            "1111000000000000000000100010000000000000000001111",
            "1000100000000000000000100010000000000000000010001",
            "1000000001000000000000100010000000000001000000001",
            "0100100110010000000000000000000000000100110010010",
            "0000001000001000000011100011100000001000001000000",
            "0000001000001000000001000001000000001000001000000",
            "0000001000001000000001111111000000001000001000000",
            "0100100110010011000010000000100001100100110010010",
            "1000000001000110000111111111110000110001000000001",
            "1000100000000011000000000000000001100000000010001",
            "1111000000000001111111111111111111000000000001111",
            "0000000000000000101000000000001010000000000000000",
            "0000000000000000000111111111110000000000000000000",
            "0000000000000000000100000000010000000000000000000",
            "0000000000000000000011111111100000000000000000000",
            "0000000000000000000000001000000000000000000000000",
            "0000000000000000000011100011100000000000000000000",
            "0000000000000000000000100010000000000000000000000",
            "0000000000000000000000000000000000000000000000000",
            "0000000000000000000001110111000000000000000000000",
            "0000000000000000000001110111000000000000000000000",
            "0000000000000000000010110110100000000000000000000",
            "0000000000000000000011100011100000000000000000000",
            "0000000000000000000001000001000000000000000000000",
        ];

        for (var y = 0; y < spaceFiller.length; y++) {
            for (var x = 0; x < spaceFiller[y].length; x++) {
                this.board[y + (this.boardHeight / 2)][x + (this.boardWidth / 2)] = spaceFiller[y][x] == "1";
            }
        }
    }
}

class Offset {
    x: number;
    y: number;

    constructor(x: number, y: number) {
        this.x = x;
        this.y = y;
    }
}

var gol = new GameOfLife();
gol.start();