

document.addEventListener('DOMContentLoaded', function(event) {

  //model


  var game = [[0,0,0], [0,0,0], [0,0,0]];
  var isFirstPlayer = true;
  var winner = null;

  var checkForWinners = function() {

    var hasWinner = false;

    //check rows
    for (var i=0; i<game.length; i++) {
      var rowSum = 0;
      var row = game[i];
      for (var t=0; t<row.length; t++) {
        rowSum += row[t];
      }
      if (rowSum === 3 || rowSum === -3) {
        hasWinner = true;
        winner = 3 ? 'First Player' : 'Second Player';
      }
    }


    //check columns
    for (var c=0; c<game[0].length; c++) {
      
      var colSum = 0;
      for (var r=0; r<game.length; r++) {
        colSum += game[r][c];
      }

      if (colSum === 3 || colSum === -3) {
        hasWinner = true;
        winner = 3 ? 'First Player' : 'Second Player';

      }

    }

    
    //check diagonals
    var leftDiag = game[0][2] + game[1][1] + game[2][0];
    var rightDiag = game[0][0] + game[1][1] + game[2][2];
    if (leftDiag === 3 || leftDiag === -3) {
      hasWinner = true;
      winner = 3 ? 'First Player' : 'Second Player';

    } else if (rightDiag === 3 || rightDiag === -3) {
      hasWinner = true;
      winner = 3 ? 'First Player' : 'Second Player';

    }
    
    console.log('haswinner?', hasWinner);

    if (hasWinner) {
      renderWinner();
    }

  }

  var handleButtonClick = function() {
    console.log('button is clicked!');
 
    //clear model board
    for (var i=0; i<game.length; i++) {
      var row = game[i];
      for (var c=0; c<row.length; c++) {
        row[c] = 0;
      }
    }

    //reset values
    isFirstPlayer = true;
    winner = null;

    //get rid of congrats
    var congratsMessage = document.getElementById('congrats');
    congratsMessage.outerHTML = '';

    //clear dom
    var squares = document.getElementsByClassName('square');
    for (var i=0; i<squares.length; i++) {
      var square = squares[i];
      square.innerHTML = '_';
    }
  

    console.log(game);



  }



  //controller

  var handleSquareClick = function(e) {

    //change model board
    var position = e.target.getAttribute('data-position').split(',');
    game[position[0]][position[1]] = isFirstPlayer ? 1 : -1;
    console.log(game);

    // renderBoard();
    var mark = isFirstPlayer ? 'X' : 'O';
    e.target.innerHTML = mark;

    //change isFirstPlayer
    isFirstPlayer = !isFirstPlayer;
    
    //check if any winners
    checkForWinners();

  }


  var squares = document.getElementsByClassName('square');

  for (var i=0; i<squares.length; i++) {
      console.log(squares[i].getAttribute('data-position'));
      var square = squares[i];
      square.addEventListener('click', handleSquareClick);
  }


  

  var button = document.getElementsByTagName('button');
  button[0].addEventListener('click', handleButtonClick);






  //view

  var renderWinner = function() {

    var message = document.createElement('div');
    message.innerHTML = 'Congrats '+winner+"!";
    message.setAttribute("id", "congrats");

    var board = document.getElementById('body');
    board.append(message);

  }


  


});