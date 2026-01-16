class Canvas {
    constructor(){
        this.ctx = document.getElementById("mycanvas").getContext("2d");
        this.width = this.ctx.width;
        this.height = this.ctx.height;
    }

    init(){
        this.fillRect(0, 0, CANVAS_RIGHT, CANVAS_BOTTOM, "rgb(245,245,245)");
        for(let i = 0; i<CANVAS_BOTTOM; i += 30){
            for(let j = 0; j<CANVAS_RIGHT; j += 30){
                this.fillRect(j, i, j+30, i+30, "rgb(200, 200, 200)");
                this.fillRect(j + 1, i + 1, j + 29, i+29, "rgb(245, 245, 245)");
            }
        }
    }

    fillRect(x1, y1, x2, y2, color){
        y1 -= 120;
        y2 -= 120;
        this.ctx.fillStyle = color;
        this.ctx.fillRect(x1, y1, x2-x1, y2-y1);
        this.ctx.fill();
    }

    fillChunk(x, y, color){
        x *= 30;
        y *= 30;
        this.fillColorBlock(x, y, color);
    }

    fillBoard(board){
        for(let i = 3; i<BOARD_HEIGHT; ++i){
            for(let j = 0; j<BOARD_WIDTH; ++j){
                this.fillChunk(j, i, board[i][j]);
            }
        }
        this.fillChunk();
    }

    fillHand(handMino){
        for(let i = 0; i<handMino.block.length; ++i){
            this.fillChunk(handMino.block[i].x, handMino.block[i].y, handMino.color);
        }
    }

    fill(){
        this.ctx.fill();
        return this;
    }

    fillColorBlock(x, y, color) {
        y -= 120;
        if(color != -1) {
            this.fillUpperBlock(x, y, color);
            this.fillSideBlock(x, y, color);
            this.fillLowerBlock(x, y, color);

        }
        else {
            this.ctx.fillStyle = 'rgb(200, 200, 200)';
            this.ctx.beginPath();
            this.ctx.moveTo(x, y);
            this.ctx.lineTo(x, y + 30);
            this.ctx.lineTo(x + 30, y + 30);
            this.ctx.lineTo(x + 30, y);
            this.ctx.closePath();
            this.ctx.fill();

            this.ctx.fillStyle = MINO_COLOR[0];
            this.ctx.beginPath();
            this.ctx.moveTo(x + 1, y + 1);
            this.ctx.moveTo(x + 30, y + 30);
            this.ctx.lineTo(x + 30, y);
            this.ctx.closePath();
            this.ctx.fill();

        }
    }

    fillUpperBlock(x, y, color){
        this.ctx.fillStyle = MINO_LIGHT_COLOR[color + 1];
        this.ctx.beginPath();
        this.ctx.moveTo(x, y);
        this.ctx.lineTo(x + 30, y);
        this.ctx.lineTo(x + 15, y + 15);
        this.ctx.closePath();
        this.ctx.fill();
    }

    fillSideBlock(x, y, color){
        this.ctx.fillStyle = MINO_COLOR[color + 1];
        this.ctx.beginPath();
        this.ctx.moveTo(x, y);
        this.ctx.lineTo(x + 30, y + 30);
        this.ctx.lineTo(x + 30, y);
        this.ctx.lineTo(x, y + 30);
        this.ctx.closePath();
        this.ctx.fill();
    }

    fillUpperBlock(x, y, color){
        this.ctx.fillStyle = MINO_DARK_COLOR[color + 1];
        this.ctx.beginPath();
        this.ctx.moveTo(x, y + 30);
        this.ctx.lineTo(x + 15, y + 15);
        this.ctx.lineTo(x + 30, y + 30);
        this.ctx.closePath();
        this.ctx.fill();
    }
}