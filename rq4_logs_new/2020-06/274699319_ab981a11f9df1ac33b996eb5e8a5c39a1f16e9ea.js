

let player1 = true;
let player2 = false;
let gameBoard = [
    [null,null,null],
    [null,null,null],
    [null,null,null]
];

function draw(box){
    if (player1){
        document.getElementById(box).innerHTML = "X";
        gamePlay(box);
        win();
        player1 = false;
        player2 = true;
        
    }else if(player2){
        document.getElementById(box).innerHTML = "O";
        gamePlay(box);
        win();
        player1 = true;
        player2 = false;
    }
    
}


function gamePlay(boxId){
    if (boxId == "box1"){
        player1 ? gameBoard[0][0] = 1 : gameBoard[0][0] = 0;
    }else if (boxId == "box2"){
        player1 ? gameBoard[0][1] = 1 : gameBoard[0][1] = 0;
    }else if (boxId == "box3"){
        player1 ? gameBoard[0][2] = 1 : gameBoard[0][2] = 0;
    }else if (boxId == "box4"){
        player1 ? gameBoard[1][0] = 1 : gameBoard[1][0] = 0;
    }else if (boxId == "box5"){
        player1 ? gameBoard[1][1] = 1 : gameBoard[1][1] = 0;
    }else if (boxId == "box6"){
        player1 ? gameBoard[1][2] = 1 : gameBoard[1][2] = 0;
    }else if (boxId == "box7"){
        player1 ? gameBoard[2][0] = 1 : gameBoard[2][0] = 0;
    }else if (boxId == "box8"){
        player1 ? gameBoard[2][1] = 1 : gameBoard[2][1] = 0;
    }else if (boxId == "box9"){
        player1 ? gameBoard[2][2] = 1 : gameBoard[2][2] = 0;
    }
}

function win(){
    //Horizontal
    for (let line of gameBoard){
        if (line[0] && line[1] && line[2]){
            console.log("player1 wins");
        }else if ((line[0] == 0) && (line[1] == 0) && (line[2] == 0)){
            console.log("player2 wins");
        }
    }

    for (let i = 0; i<3;i++){
        //vertical
        if (gameBoard[0][i] && gameBoard[1][i] && gameBoard[2][i]){
            console.log("player1 wins");
        }else if ((gameBoard[0][i] == 0) && (gameBoard[1][i] == 0) && (gameBoard[2][i] == 0)){
            console.log("player2 wins");
        }
    }

    //Diagonals
    if (gameBoard[0][0] && gameBoard[1][1] && gameBoard[2][2]){
        console.log("player1 wins");
    }else if ((gameBoard[0][0] == 0) && (gameBoard[1][1] == 0) && (gameBoard[2][i] == 0)){
        console.log("player2 wins");
    }else if (gameBoard[0][2] && gameBoard[1][1] && gameBoard[2][0]){
        console.log("player1 wins");
    }else if ((gameBoard[0][2] == 0) && (gameBoard[1][1] == 0) && (gameBoard[2][0] == 0)){
        console.log("player2 wins");
    }
}