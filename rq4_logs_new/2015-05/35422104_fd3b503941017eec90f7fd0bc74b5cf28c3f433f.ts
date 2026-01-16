module MaruBatsu {
    export enum Piece {
        Maru,
        Batsu
    }

    export class GameLogic {
        SIDE = 3;
        board:Piece[][];
        turn:Piece;

        constructor(public el:HTMLElement) {

        }

        start():void {
            this.el.innerHTML = "";
            this.turn = Piece.Maru;
            this.board = [];

            for (var i = 0; i < this.SIDE; i++) {
                this.board.push([]);
            }

            for (var y = 0; y < this.SIDE; y++) {
                for (var x = 0; x < this.SIDE; x++) {
                    this.board[x][y] = null;
                    this.addPiece(x, y);
                }
            }
        }

        put(x:number, y:number, el:HTMLElement):void {
            if (this.board[x][y] !== null) {
                return;
            }

            var current = this.turn;
            this.board[x][y] = this.turn;

            if (this.turn === Piece.Maru) {
                el.innerText = '●';
                this.turn = Piece.Batsu;
            } else {
                el.innerText = '☓';
                this.turn = Piece.Maru;
            }
            if (this.isWinner(current)) {
                if (current === Piece.Maru) {
                    alert('● win');
                    this.end();
                } else {
                    alert('☓ win');
                    this.end();
                }
            } else if (this.isDraw(current)) {
                alert('even');
                this.end();
            } else {
                console.log('put: ' + x + ',' + y);
            }
        }

        isWinner(turn:Piece):boolean {
            for (var i = 0; i < this.SIDE; i++) {
                var v = true;
                var h = true;

                for (var j = 0; j < this.SIDE; j++) {
                    console.log('check: ' + i + ',' + j + ' = ' + this.board[i][j]);
                    console.log('check: ' + j + ',' + i + ' = ' + this.board[j][i]);
                    if (this.board[i][j] !== turn) {
                        v = false;
                    }
                    if (this.board[j][i] !== turn) {
                        h = false;
                    }
                }

                if (v || h) {
                    return true;
                }
            }


            var c = true;
            var r = true;

            for (var i = 0; i < this.SIDE; i++) {
                if (this.board[i][i] !== turn) {
                    c = false;
                }

                if (this.board[i][this.SIDE - i - 1] !== turn) {
                    r = false;
                }
            }

            if (c || r) {
                return true;
            }

            return false;
        }

        isDraw(turn:Piece):boolean {
            for (var x = 0; x < this.SIDE; x++) {
                for (var y = 0; y < this.SIDE; y++) {
                    if (this.board[x][y] === null) {
                        return false;
                    }
                }
            }

            return true;
        }

        addPiece(x:number, y:number):void {
            var div = document.createElement('div');
            div.addEventListener('click', () => {
                this.put(x, y, div);
            });

            div.className = 'piece';

            if (x === 0) {
                div.className += ' head';
            }

            this.el.appendChild(div);
        }

        end():void {
            this.start();
        }
    }
}

window.onload = () => {
    var el = document.getElementById('game');
    var logic = new MaruBatsu.GameLogic(el);
    logic.start();
};