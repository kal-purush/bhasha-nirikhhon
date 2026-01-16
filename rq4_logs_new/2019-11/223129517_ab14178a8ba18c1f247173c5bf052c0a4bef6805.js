'use strict'
var gBoard = creatBoard(4, 4)
var randlocs = createRandLoc(gBoard, 2)
var FLAG = '🚩'
var MINE = 'X'



var cell = {
    minesArroundCount: 0,
    isMine: false,
    isShown: false,
    isMarked: false
}

function init() {
    //preventDefault()    
    setMinesNegsCount(gBoard)
    renderBoard()



}





function creatBoard(rows, cols) {

    var board = [];
    for (var i = 0; i < rows; i++) {
        board[i] = [];
        for (var j = 0; j < cols; j++) {
            var cell = {
                minesArroundCount: 0,
                isMine: false,
                isShown: false,
                isMarked: false
            }
            // if (i === 0 && j === 1) { cell.isMine = true }
            // if (i === 1 && j === 3) { cell.isMine = true }

            board[i][j] = cell

        }
    }

    return board
}

function setMines(board, arrOfRandLocs) {
    for (var i = 0; i < arrOfRandLocs.length; i++) {
        board[arrOfRandLocs[i].i][arrOfRandLocs[i].j].isMine = true
    }
}
setMines(gBoard, randlocs)
console.log(gBoard)





console.log(randlocs)

function countNeighbors(board, pos) {
    //    debugger
    var mineCount = 0;
    for (var i = pos.i - 1; i <= pos.i + 1; i++) {

        if (i < 0 || i > board.length - 1) continue;

        for (var j = pos.j - 1; j <= pos.j + 1; j++) {

            if (j < 0 || j > board[0].length - 1) continue;

            if (i === pos.i && j === pos.j) continue;
            if (board[i][j].isMine) mineCount++;
        }
    }
    return mineCount


}

function setMinesNegsCount(board) {
    var pos = { i: '', j: '' }
    for (var i = 0; i < board.length; i++) {
        for (var j = 0; j < board[0].length; j++) {
            var cell = board[i][j]
            pos.i = i
            pos.j = j
            cell.minesArroundCount = countNeighbors(board, pos)
        }


    }

}

function renderBoard() {
    var elGrid = document.querySelector('.table')
    var strHTML = '<tbody>\n'
    for (var i = 0; i < gBoard.length; i++) {
        strHTML += '<tr>\n'
        for (var j = 0; j < gBoard.length; j++) {
            var cell = gBoard[i][j]
            var className = `cell `
            className += (cell.isShown) ? `shown` : ''
            className += (cell.isMarked) ? `marked ` : ''
            strHTML += `<td class="${className}" data-i=${i}  data-j=${j} onclick="cellClicked(this,${i},${j})"
            oncontextmenu="cellMarked(this,event,${i},${j})">`
            //strHTML += `${(cell.isMine) ? 'x' : cell.minesArroundCount}</td>\n`

            if (!cell.isMarked && !cell.isShown&& !cell.isMine){ strHTML += `${cell.minesArroundCount}</td>` }
            if (!cell.isMarked && !cell.isShown&& cell.isMine){ strHTML += `${MINE}</td>` }
            if (!cell.isMarked && cell.isShown&& !cell.isMine){ strHTML += `${cell.minesArroundCount}</td>` }
            if (cell.isMarked && !cell.isShown&& !cell.isMine){ strHTML += `${FLAG}</td>` }
            if (cell.isMarked && !cell.isShown&& cell.isMine){ strHTML += `${FLAG}</td>` }
            if (cell.isMarked && cell.isShown&& !cell.isMine){ strHTML += `${cell.minesArroundCount}</td>` }
            if (cell.isMarked && cell.isShown&& cell.isMine){ strHTML += `${MINE}</td>` }
            if (!cell.isMarked && cell.isShown&& cell.isMine){ strHTML += `${MINE}</td>` }              



        }
        strHTML += '</tr>\n'
    }
    strHTML += '</tbody>'
    console.log(strHTML)
    console.log(gBoard)
    elGrid.innerHTML = strHTML


}

// function fillCell(cell, string) {
//     debugger
//     if (cell.isMarked && !cell.isShown) {
//         string += FLAG
//     }
//     else if (cell.isShown) {
//         if (cell.isMine) { string += 'x' }
//         else { string += cell.minesArroundCount }
//     }
//     return string

// }

function cellMarked(elCell, ev, posI, posJ) {
    ev.preventDefault();
    console.log(ev)
    var curCell = gBoard[posI][posJ]
    curCell.isMarked = true
    renderBoard()
    
    // renderBoard()

}

function cellClicked(elCell, posI, posJ, event) {

    var curCell = gBoard[posI][posJ]
    if (!curCell.isMine) {
        curCell.isShown = true
        renderBoard()
    }
    else {
        //search all mines on board
        for (var i = 0; i < gBoard.length; i++) {
            for (var j = 0; j < gBoard.length; j++) {
                if (gBoard[i][j].isMine) {
                    gBoard[i][j].isShown = true
                    //game over
                }
            }

        }
        renderBoard()
    }

}

// function preventDefault(){
//     var elTable=document.querySelector('table');
//     elTable.addEventListener('contextmenu', e => {
//         e.preventDefault();
//       })
// }






// function renderCinema() {
//     var strHTML = '';
//     for (var i = 0; i < gCinema.length; i++) {
//         strHTML += '<tr>\n'
//         for (var j = 0; j < gCinema[i].length; j++) {
//             var cell = gCinema[i][j];

//             var className = (cell.type === 'EMPTY') ? '' : 'seat '
//             className += (cell.taken) ? 'taken ' : '';
//             className += (cell.selected) ? 'selected ' : '';

//             strHTML += `\t<td
//                 data-i=${i}  data-j=${j}
//                 class="${className}" onclick="seatClicked(this, ${i}, ${j})"></td>\n`
//         }
//         strHTML += '</tr>\n'
//     }

//      console.log(strHTML)
//     var elSeats = document.querySelector('.seats');
//     elSeats.innerHTML = strHTML;
// }