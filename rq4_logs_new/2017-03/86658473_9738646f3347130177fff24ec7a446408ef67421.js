documentWidth = window.screen.availWidth;
gridContainerWidth = 0.92 * documentWidth;
cellSideLength = 0.18 * documentWidth;
cellSpace = 0.04 * documentWidth;

function getPosTop(i,j){
    return cellSpace + i*(cellSpace + cellSideLength);
}

function getPosLeft(i,j){
    return cellSpace + j*(cellSpace + cellSideLength);
}

function getNumberBackgroundImage(number){
    switch (number){
        case 2: return "url(02.JPG)";break;
        case 4: return "url(04.JPG)";break;
        case 8: return "url(08.JPG)";break;
        case 16: return "url(16.JPG)";break;
        case 32: return "url(32.JPG)";break;
        case 64: return "url(64.JPG)";break;
        case 128: return "url(128.JPG)";break;
        case 256: return "url(256.JPG)";break;
        case 512: return "url(512.JPG)";break;
        case 1024: return "url(1024.JPG)";break;
        case 2048: return "url(2048.JPG)";break;
        case 4096: return "#a6c";break;
        case 8192: return "#93c";break;
    }

    return "black";
}

function getNumberColor(number){
    if( number <= 4){
        return"#776e65"
    }
    return "white";
}

function nospace(board){
    for(var i = 0; i < 4; i++) {
        for (var j = 0; j < 4; j++) {
            if (board[i][j] == 0) {
                return false;
            }
        }
    }
    return true;
}

function canMoveLeft( board ){
    for( var i = 0; i < 4; i ++ ){
        for( var j = 1; j < 4; j++ ){
            if( board[i][j] !=0 ){
                if( board[i][j-1] == 0 || board[i][j-1] == board[i][j] ){
                    return true;
                }
            }
        }
    }
    return false;
};

function canMoveRight( board ){
    for( var i = 0; i < 4; i ++ ){
        for( var j = 2; j >= 0; j-- ){
            if( board[i][j] != 0 ){
                if( board[i][j+1] == 0 || board[i][j+1] == board[i][j] ){
                    return true;
                }
            }
        }
    }
    return false;
};

function canMoveUp( board ){
    for( var j = 0; j < 4; j ++ ){
        for( var i = 1; i < 4; i ++ ){
            if( board[i][j] != 0 ){
                if( board[i-1][j] == 0 || board[i-1][j] == board[i][j] ){
                    return true;
                }
            }
        }
    }
    return false;
};

function canMoveDown( board ){
    for( var j = 0; j < 4; j++ ){
        for( var i = 2; i >= 0; i-- ){
            if( board[i][j] != 0 ){
                if( board[i+1][j] == 0 || board[i+1][j] == board[i][j] ){
                    return true;
                }
            }
        }
    }
    return false;
};

function noBlockHorizontal( row, col1, col2, board ){
    for( var i = col1+1; i < col2; i++ ){
        if( board[row][i] !=0 ){
            return false;
        }
    }
    return true;
};

function noBlockVertical( col, row1, row2, board ){
    for( var i = row1+1; i < row2; i++ ){
        if( board[i][col] != 0 ){
            return false;
        }
    }
    return true;
};

function nomove( board ){
    if( canMoveDown( board ) || canMoveLeft( board ) || canMoveRight( board ) || canMoveUp( board )){
        return false;
    }else{
        return true;
    }
}