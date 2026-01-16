import { Stone } from "./stone";
import { StonePosition } from "./stonePosition";
import { Blocks } from "./blocks";
import { I, J, L, O, S, T, Z } from "./stones";


export class Game {

    private levelSpeedMS = 1000;  // Smaller is faster is harder
    private tickTimer: number;
    private lastUpdateTime: number;

    private gameLoopIntervalId: any;

    private readonly BOARD_WIDTH = 10;
    private readonly BOARD_HEIGHT = 17;
    private readonly BOARD_UNIT_SIZE = 30;

    private canvas: HTMLCanvasElement;
    private ctx: CanvasRenderingContext2D;

    private board: number[][];
    private currentStone: Stone;

    public constructor() {
        this.canvas = <HTMLCanvasElement>document.getElementById('board');
        this.ctx = this.canvas.getContext('2d');

        let gameContainer = document.getElementById('gameContainer');
        gameContainer.addEventListener("keydown", this.handleKeyDown);

        this.initBoard();

        this.createStone();

        this.initGameLoop();
    }

    private handleKeyDown = (e: KeyboardEvent) => {
        switch (e.keyCode) {
            case 37: {
                // Left
                this.moveStoneLeft(this.currentStone);
                break;
            }
            case 38: {
                // Up
                this.instantDown(this.currentStone);
                break;
            }
            case 39: {
                // Right
                this.moveStoneRight(this.currentStone);
                break;
            }
            case 40: {
                // Down
                this.moveStoneDown(this.currentStone);
                break;
            }
            case 65: {
                // Rotate CW
                if (this.currentStone) {
                    this.currentStone.rotateCCW();
                }
                break;
            }
            case 68: {
                // Rotate CCW
                if (this.currentStone) {
                    this.currentStone.rotateCW();
                }
                break;
            }
        }

        e.preventDefault();
        e.stopPropagation();
    }

    private initGameLoop() {
        this.lastUpdateTime = Date.now();
        this.tickTimer = this.levelSpeedMS;

        let fps = 60;
        // Start the game loop
        this.gameLoopIntervalId = setInterval(this.run, 1000 / fps);
    }

    public stop() {
        this.levelSpeedMS = 10000000;
        //clearInterval(this.gameLoopIntervalId);

        //this.clear();
    }

    private instantDown = (stone: Stone) => {
        // TODO
    }

    private createStone() {
        let newStoneType: Blocks;
        newStoneType = Math.floor(Math.random() * 7) + 1;

        switch (newStoneType) {
            case Blocks.i: {
                this.currentStone = new I();
                break;
            }
            case Blocks.j: {
                this.currentStone = new J();
                break;
            }
            case Blocks.l: {
                this.currentStone = new L();
                break;
            }
            case Blocks.o: {
                this.currentStone = new O();
                break;
            }
            case Blocks.s: {
                this.currentStone = new S();
                break;
            }
            case Blocks.t: {
                this.currentStone = new T();
                break;
            }
            case Blocks.z: {
                this.currentStone = new Z();
                break;
            }
        }
    }

    public moveStoneLeft(stone: Stone) {
        if (!stone) {
            return;
        }

        // Check if the stone can be moved left
        for (let i = 0; i < stone.positions.length; i++) {
            if (stone.positions[i].x - 1 < 0 || this.doesPositionCollide(stone.positions[i].x - 1, stone.positions[i].y)) {
                return;
            }
        }
        // Move the stone left
        for (let i = 0; i < stone.positions.length; i++) {
            stone.positions[i].x--;
        }
    }

    private moveStoneRight(stone: Stone) {
        if (!stone) {
            return;
        }

        // Check if the stone can be moved right
        for (let i = 0; i < stone.positions.length; i++) {
            if (stone.positions[i].x + 1 >= this.BOARD_WIDTH || this.doesPositionCollide(stone.positions[i].x + 1, stone.positions[i].y)) {
                return;
            }
        }
        // Move the stone left
        for (let i = 0; i < stone.positions.length; i++) {
            stone.positions[i].x++;
        }
    }

    private moveStoneDown(stone: Stone) {
        if (!stone) {
            return;
        }

        // Check if the stone can be moved down
        for (let i = 0; i < stone.positions.length; i++) {
            if (stone.positions[i].y + 1 >= this.BOARD_HEIGHT || this.doesPositionCollide(stone.positions[i].x, stone.positions[i].y + 1)) {
                this.freezeStone();
                return;
            }
        }
        // Move the stone down
        for (let i = 0; i < stone.positions.length; i++) {
            stone.positions[i].y++;
        }
    }

    private initBoard() {
        // Init model
        this.board = [];
        for (let i = 0; i < this.BOARD_WIDTH; i++) {
            this.board[i] = [];
            for (let j = 0; j < this.BOARD_HEIGHT; j++) {
                this.board[i][j] = Blocks.undefined;
            }
        }

        // Init Canvas
        this.canvas.width = this.BOARD_WIDTH * this.BOARD_UNIT_SIZE;
        this.canvas.height = this.BOARD_HEIGHT * this.BOARD_UNIT_SIZE;
    }

    private run = () => {
        this.update();
        this.draw();
    }

    private update = () => {
        // Calculate delta time in milliseconds
        let dt = Date.now() - this.lastUpdateTime;
        this.tickTimer -= dt;
        if (this.tickTimer <= 0) {

            // A game step can be performed
            this.performWorldStep();

            this.tickTimer = this.levelSpeedMS;
        }

        this.lastUpdateTime = Date.now();
    }

    private performWorldStep = () => {
        this.moveStoneDown(this.currentStone);
    }

    private doesPositionCollide = (x: number, y: number): boolean => {
        return this.board[x][y] != Blocks.undefined;
    }

    private draw = () => {
        this.clear();
        this.drawBoard();
        this.drawStone(this.currentStone);
    }

    private clear = () => {
        this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);
    }

    private freezeStone = () => {
        this.freezeCurrentStone();
        this.checkForFullLines();
        this.createStone();
    }

    private freezeCurrentStone = () => {
        for (let i = 0; i < this.currentStone.positions.length; i++) {
            this.board[this.currentStone.positions[i].x][this.currentStone.positions[i].y] = this.currentStone.stoneType;
        }
    }

    private checkForFullLines = () => {

        let fullLinesIndices = new Array<number>();
        for (let j = 0; j < this.BOARD_HEIGHT; j++) {
            let isFullLine = true;
            for (let i = 0; i < this.BOARD_WIDTH; i++) {
                if (this.board[i][j] === Blocks.undefined) {
                    isFullLine = false;
                    break;
                }
            }
            if (isFullLine) {
                fullLinesIndices.push(j);
            }
        }

        this.removeLines(fullLinesIndices);
    }

    private copyLine = (sourceIndex: number, targetIndex: number) => {
        for (let i = 0; i < this.BOARD_WIDTH; i++) {
            this.board[i][targetIndex] = this.board[i][sourceIndex];
        }
    }

    private removeLines = (indices: Array<number>) => {
        if (!indices || indices.length === 0) {
            return;
        }

        // Two pointers, to select the source and target lines for copying values.
        let targetIndex: number;
        let sourceIndex: number;
        let offset = 1;

        // Remove lines by overwrite lines which should be removed with valid lines.
        // Start at the last line which should be deleted and go up from there...
        for (targetIndex = indices[indices.length - 1]; targetIndex >= 0; targetIndex--) {
            // Target line should be removed
            for (sourceIndex = targetIndex - offset; sourceIndex >= 0; sourceIndex--) {
                if (indices.indexOf(sourceIndex) === -1) {
                    // A valid source line was found.
                    this.copyLine(sourceIndex, targetIndex);
                    break;
                } else {
                    offset++;
                }
            }
        }

    }

    private drawBoard = () => {
        for (let i = 0; i < this.BOARD_WIDTH; i++) {
            for (let j = 0; j < this.BOARD_HEIGHT; j++) {
                if (this.board[i][j] !== Blocks.undefined) {
                    this.drawBlock(i, j, this.board[i][j]);
                }
            }
        }
    }

    private drawStone = (stone: Stone) => {
        if (!stone) {
            return;
        }
        stone.positions.forEach((position) => {
            this.drawBlock(position.x, position.y, stone.stoneType);
        });
    }

    private drawBlock = (x: number, y: number, blockType: Blocks) => {
        this.ctx.beginPath();
        this.ctx.lineWidth = 2;
        this.ctx.rect(x * this.BOARD_UNIT_SIZE, y * this.BOARD_UNIT_SIZE, this.BOARD_UNIT_SIZE, this.BOARD_UNIT_SIZE);

        switch (blockType) {
            case Blocks.i: {
                this.ctx.strokeStyle = "green";
                break;
            }
            case Blocks.j: {
                this.ctx.strokeStyle = "red";
                break;
            }
            case Blocks.l: {
                this.ctx.strokeStyle = "blue";
                break;
            }
            case Blocks.o: {
                this.ctx.strokeStyle = "yellow";
                break;
            }
            case Blocks.s: {
                this.ctx.strokeStyle = "black";
                break;
            }
            case Blocks.t: {
                this.ctx.strokeStyle = "purple";
                break;
            }
            case Blocks.z: {
                this.ctx.strokeStyle = "lightblue";
                break;
            }
        }

        this.ctx.stroke();
    }

}