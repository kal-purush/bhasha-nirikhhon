function Mine(tr, td, mineNum) {
    this.tr = tr;
    this.td = td;
    this.numNum = mineNum;

    this.squares = [];
    this.tds = [];
    this.surplusMine = mineNum;
    this.allRight = false;

    
    this.parent = document.querySelector('.gameBox');



}
Mine.prototype.randomNum = function () {
    var square = new Array(this.tr * this.td);
    for (var i = 0; i < square.length; i++) {
        square[i] = i;

    }
    square.sort(function () { return 0.5 - Math.random() });
    //console.log(square);
    return square.slice(0, this.numNum);

}

Mine.prototype.init = function () {
    var rn = this.randomNum();
    //console.log(rn);
    var n = 0;
    for (var i = 0; i < this.tr; i++) {
        this.squares[i] = [];
        for (var j = 0; j < this.td; j++) {
            n++;
            if (rn.indexOf(n) !=-1) {
                this.squares[i][j] = {
                    type: 'mine',
                    x: j,
                    y: i
                };
            } else {
                this.squares[i][j] = {
                    type: 'number',
                    x: j,
                    y: i,
                    value: 0
                };
            }
        }
    }
    mine.squares = this.squares;
    //console.log(this.squares);
    this.parent.oncontextmenu = function () {
        return false;
    }



    this.upDateNum();
    this.createDom();

    this.mineNumDom = document.querySelector('.mineNum');
    this.mineNumDom.innerHTML = this.surplusMine;
    


};
Mine.prototype.createDom = function () {
    var This = this;
    var table = document.createElement('table');
    for (var i = 0; i < this.tr; i++) {
        var domTr = document.createElement('tr');
        this.tds[i] = [];
        for (var j = 0; j < this.td; j++) {
            var domTd = document.createElement('td');
            //domTd.innerHTML = 0;

            domTd.pos = [i, j];
            domTd.onmousedown = function () {
                    This.play(event, this);

            }
            this.tds[i][j] = domTd;
            /*if (this.squares[i][j].type == 'mine') {
                domTd.className = 'mine';
            }
            if (this.squares[i][j].type == 'number') {
                domTd.innerHTML=this.squares[i][j].value;
            }*/
            domTr.appendChild(domTd);
        }
        table.appendChild(domTr);
    }
    this.parent.innerHTML = '';
    this.parent.appendChild(table); 
}
Mine.prototype.getAround = function (squares) {
    var x = squares.x;
    var y = squares.y;
    var result = [];
    for (var i = x - 1; i <= x + 1; i++) {
        for (var j = y - 1; j <= y + 1; j++) {
            if (
                i < 0 ||
                j < 0 ||
                i > this.td - 1 ||
                j > this.tr - 1 ||
                (i == x && j == y) ||
                this.squares[j][i].type == 'mine'

            ) {
                continue;

            }
            result.push([j, i]);
        }
    }

    return result;


};
Mine.prototype.upDateNum = function () {
    for (var i = 0; i < this.tr; i++) {
        for (var j = 0; j < this.td; j++) {
            if (this.squares[i][j].type == 'number') {
                continue;
            }
            var num = this.getAround(this.squares[i][j]);
            for (var k = 0; k < num.length; k++) {
                //console.log(this.squares[num[k][0]][num[k][1]].value);
                this.squares[num[k][0]][num[k][1]].value += 1;
            }
        }
    }
    //console.log(this.squares);
}
Mine.prototype.play = function (ev, obj) {
    var This = this;
    if (ev.which == 1 && obj.className != 'flag') {
        var curSquare = this.squares[obj.pos[0]][obj.pos[1]];
        var cl = ['zero', 'one', 'two','three','four','five','six','seven','eight']
        if (curSquare.type == 'number') {
            obj.innerHTML = curSquare.value;
            obj.className = cl[curSquare.value];
            if (curSquare.value == 0) {
                obj.innerHTML = '';

                function getAllZero(square) {
                    var around = This.getAround(square);
                    //console.log(around);
                    for (var i = 0; i < around.length; i++) {
                        var x = around[i][0];
                        var y = around[i][1];
                        This.tds[x][y].className = cl[This.squares[x][y].value];
                        if (This.squares[x][y].value == 0) {
                            //console.log(x);
                            //console.log(y);
                            if (!This.tds[x][y].check) {
                                This.tds[x][y].check = true;
                                getAllZero(This.squares[x][y]);
                            }

                        } else {
                            This.tds[x][y].innerHTML = This.squares[x][y].value;
                        }
                    }
                }
                getAllZero(curSquare)
            }

        } else {
            this.gameOver(obj);
            //console.log("its a mine");

        }
        //console.log(curSquare);
    }
    if (ev.which == 3) {
        if (obj.className && obj.className != 'flag') {
            return;
        }
        obj.className = obj.className == 'flag' ? '' : 'flag';
        if (obj.className == 'flag') {
            --this.surplusMine;
        } else {
            ++this.surplusMine;
        }
        this.mineNumDom.innerHTML = this.surplusMine;

    }
    //console.log(obj);

}

Mine.prototype.gameOver = function (clickTd) {
    for (var i = 0; i < this.tr; i++) {
        for (var j = 0; j < this.td; j++) {
            if (this.squares[i][j].type == 'mine') {
                this.tds[i][j].className = 'mine';

            }
            this.tds[i][j].onmousedown = null;
        }
        
    }
    if (clickTd) {
        clickTd.style.backgroundColor= '#F00';
    }
}
var btns = document.querySelectorAll('.level button');
var mine = null;
var ln = 0;
var arr=[[9, 9, 10], [16, 16, 40], [28, 28, 99]];
for (let i = 0; i < btns.length-1; i++) {
    btns[i].onclick = function () {
        for (var k = 0; k < btns.length; k++) {
            btns[k].className = '';
            this.className = 'active';
        }
        mine = new Mine(arr[i][0], arr[i][1], arr[i][2]);
        mine.init();
        //console.log(i);
        
    }
}

btns[0].onclick();
btns[3].onclick = function () {
    btns[0].onclick();
}
btns[3].onclick

/*var mine = new Mine(10, 10, 5);
mine.init();*/
//console.log(mine.getAround(mine.squares[0][0]));