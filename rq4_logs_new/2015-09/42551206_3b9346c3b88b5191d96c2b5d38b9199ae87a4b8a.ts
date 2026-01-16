/// <reference path="obelisk.d.ts"/>

class GameOfLifeObelisk {
    private gridSize: number = 12;
    private boardWidth: number = 240;
    private boardHeight: number = 200;
    private boardState = new Uint8Array(this.boardWidth * this.boardHeight);
    private cubeSide: number = this.gridSize;
    private cubeHeight: number = this.gridSize;
    private pixelView: obelisk.PixelView;
    private colorSeed: number = 20;

    private brick = new obelisk.Brick(new obelisk.BrickDimension(this.gridSize, this.gridSize), undefined, false);
    private cubeDimension: obelisk.CubeDimension = new obelisk.CubeDimension(this.cubeSide, this.cubeSide, this.cubeHeight);

    constructor() {
        this.pixelView = new obelisk.PixelView(<HTMLCanvasElement>document.getElementById('board'), new obelisk.Point(0, -1000));
        this.pixelView.context.canvas.width = window.innerWidth - 20;
        this.pixelView.context.canvas.height = window.innerHeight - 20;
    }

    public start() {
        this.seedBoard();
        this.nextFrame();
    }

    private nextFrame() {
        this.render();
        this.updateBoard();
        requestAnimationFrame(this.nextFrame.bind(this));
    }

    private seedBoard() {
        for (var i = 0; i < this.boardState.length; i++) {
            if (Math.random() > 0.78) {
                this.boardState[i] = 1;
            }
        }
    }

    private generateColor() {
        var red: number = Math.sin(0.05 * this.colorSeed) * 127 + 128;
        var grn: number = Math.sin(0.10 * this.colorSeed) * 127 + 128;
        var blu: number = Math.sin(0.15 * this.colorSeed) * 127 + 128;

        return (Math.floor(red) << 16) + (Math.floor(grn) << 8) + (Math.floor(blu));
    }

    private generateCubeWithColor(): obelisk.Cube {
        var newColor: number = this.generateColor();
        var newCubeColor = new obelisk.CubeColor().getByHorizontalColor(newColor);
        return new obelisk.Cube(this.cubeDimension, newCubeColor, false);
    }

    private cellAliveAt(x, y): number {
        if (x >= 0 && x < this.boardWidth &&
            y >= 0 && y < this.boardHeight) {
            return this.boardState[(y * this.boardWidth) + x];
        } else {
            return 0;
        }
    }

    private getCountOfAliveNeighbors(x, y): number {
        var count =
            this.cellAliveAt(x - 1, y - 1) +
            this.cellAliveAt(x    , y - 1) +
            this.cellAliveAt(x + 1, y - 1) +

            this.cellAliveAt(x - 1, y    ) +
            this.cellAliveAt(x + 1, y    ) +

            this.cellAliveAt(x - 1, y + 1) +
            this.cellAliveAt(x    , y + 1) +
            this.cellAliveAt(x + 1, y + 1);

        return count;
    }

    private updateBoard() {
        var nextBoard = new Uint8Array(this.boardWidth * this.boardHeight);

        for (var x = 0; x < this.boardWidth; x++) {
            for (var y = 0; y < this.boardHeight; y++) {

                var prevState: number = this.boardState[(y * this.boardWidth) + x];
                var nextState: number = 0;
                var aliveNeighbors: number = this.getCountOfAliveNeighbors(x, y);

                if (prevState === 1 && (aliveNeighbors === 2 || aliveNeighbors === 3)) {
                    nextState = 1;
                }
                else if (prevState === 0 && aliveNeighbors === 3) {
                    nextState = 1;
                }

                nextBoard[(y * this.boardWidth) + x] = nextState;
            }
        }

        this.boardState = nextBoard;
    }

    private render() {
        this.colorSeed++;
        var cube: obelisk.Cube = this.generateCubeWithColor();

        this.pixelView.clear();
        for (var x = 0; x < this.boardWidth; x++) {
            for (var y = 0; y < this.boardHeight; y++) {

                var point3d = new obelisk.Point3D(x * this.gridSize, y * this.gridSize, 0);

                if (this.boardState[(y * this.boardWidth) + x] === 1) {
                    this.pixelView.renderObject(cube, new obelisk.Point3D(x * this.gridSize, y * this.gridSize, 0))
                } else {
                    this.pixelView.renderObject(this.brick, point3d);
                }

            }
        }
    }
}

var game = new GameOfLifeObelisk();
game.start();