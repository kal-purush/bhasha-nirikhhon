window.onload = function() {
    // 绘制棋盘
    boardRendering();

    cvs.onclick = function(e) {
        if (gameOver) return;
        var i = Math.floor(e.offsetX / 30);
        var j = Math.floor(e.offsetY / 30);
        if (board[i][j] === 0) {
            moves.push({
                x: i,
                y: j,
                color: pieceColor
            });
            // 悔棋后又下了一步则之前存储的已悔棋清空，无法再撤销
            retractions = [];
            pieceRendering(i, j, pieceColor);
        }
    }

    retractBtn.onclick = function() {
        retract();
    }

    abortRetractBtn.onclick = function() {
        abortRetract();
    }

    restartBtn.onclick = function() {
        window.location.reload();
    }
}