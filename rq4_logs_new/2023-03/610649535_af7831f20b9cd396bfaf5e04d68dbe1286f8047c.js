document.addEventListener('DOMContentLoaded', () => {
    const grid = document.querySelector('.grid');
    let squares = Array.from(document.querySelectorAll('.grid div'));
    const scoreDisplay = document.querySelector('#score');
    const startBtn = document.querySelector('#start-button'); 
    const highestScoreConatiner = document.querySelector('#highestScoreConatiner'); 
    const restartBtn = document.getElementById("restart");
    const audioBtn = document.getElementById("music");
    const level = document.getElementById("level");
    const nameContainer = document.getElementById('nameContainer');
    const width = 10
    let nextRandom = 0
    let timerId
    let score = 0
    let speed = 1000
    let playerName = null;
    const colors = [
      'orange',
      'red',
      'purple',
      'green',
      'blue'
    ]

    const displayHighestScore = () => {
      let highestScore = localStorage.getItem('scoreBoard');
      if(highestScore) {
        highestScore = JSON.parse(highestScore);
        highestScoreConatiner.innerHTML = highestScore.score; 
      }
    }

    displayHighestScore();
  
    //The Tetrominoes
    const lTetromino = [
      [1, width+1, width*2+1, 2],
      [width, width+1, width+2, width*2+2],
      [1, width+1, width*2+1, width*2],
      [width, width*2, width*2+1, width*2+2]
    ]
  
    const zTetromino = [
      [0,width,width+1,width*2+1],
      [width+1, width+2,width*2,width*2+1],
      [0,width,width+1,width*2+1],
      [width+1, width+2,width*2,width*2+1]
    ]
  
    const tTetromino = [
      [1,width,width+1,width+2],
      [1,width+1,width+2,width*2+1],
      [width,width+1,width+2,width*2+1],
      [1,width,width+1,width*2+1]
    ]
  
    const oTetromino = [
      [0,1,width,width+1],
      [0,1,width,width+1],
      [0,1,width,width+1],
      [0,1,width,width+1]
    ]
  
    const iTetromino = [
      [1,width+1,width*2+1,width*3+1],
      [width,width+1,width+2,width+3],
      [1,width+1,width*2+1,width*3+1],
      [width,width+1,width+2,width+3]
    ]
  
    const theTetrominoes = [lTetromino, zTetromino, tTetromino, oTetromino, iTetromino]
  
    let currentPosition = 4
    let currentRotation = 0
  
    console.log(theTetrominoes[0][0])
  
    //randomly select a Tetromino and its first rotation
    let random = Math.floor(Math.random()*theTetrominoes.length)
    let current = theTetrominoes[random][currentRotation]
  
    //draw the Tetromino
    function draw() {
      current.forEach(index => {
        squares[currentPosition + index].classList.add('tetromino')
        squares[currentPosition + index].style.backgroundColor = colors[random]
      })
    }
  
    //undraw the Tetromino
    function undraw() {
      current.forEach(index => {
        squares[currentPosition + index].classList.remove('tetromino')
        squares[currentPosition + index].style.backgroundColor = ''
  
      })
    }
  
    //assign functions to keyCodes
    function control(e) {
      if(e.keyCode === 37) {
        moveLeft()
      } else if (e.keyCode === 38) {
        rotate()
      } else if (e.keyCode === 39) {
        moveRight()
      } else if (e.keyCode === 40) {
        console.log(e)
        moveDown()
      }
    }
    document.addEventListener('keyup', control)
    document.addEventListener('keydown', (e) => {
      e.keyCode === 40 ? moveDown() : null
    })
  
    //move down function
    function moveDown() {
      undraw()
      currentPosition += width
      draw()
      freeze()
    }
  
    //freeze function
    function freeze() {
      if(current.some(index => squares[currentPosition + index + width].classList.contains('taken'))) {
        current.forEach(index => squares[currentPosition + index].classList.add('taken'))
        //start a new tetromino falling
        random = nextRandom
        nextRandom = Math.floor(Math.random() * theTetrominoes.length)
        current = theTetrominoes[random][currentRotation]
        currentPosition = 4
        draw()
        displayShape()
        addScore()
        gameOver()
      }
    }
  
    //move the tetromino left, unless is at the edge or there is a blockage
    function moveLeft() {
      undraw()
      const isAtLeftEdge = current.some(index => (currentPosition + index) % width === 0)
      if(!isAtLeftEdge) currentPosition -=1
      if(current.some(index => squares[currentPosition + index].classList.contains('taken'))) {
        currentPosition +=1
      }
      draw()
    }
  
    //move the tetromino right, unless is at the edge or there is a blockage
    function moveRight() {
      undraw()
      const isAtRightEdge = current.some(index => (currentPosition + index) % width === width -1)
      if(!isAtRightEdge) currentPosition +=1
      if(current.some(index => squares[currentPosition + index].classList.contains('taken'))) {
        currentPosition -=1
      }
      draw()
    }
  
    
    ///FIX ROTATION OF TETROMINOS A THE EDGE 
    function isAtRight() {
      return current.some(index=> (currentPosition + index + 1) % width === 0)  
    }
    
    function isAtLeft() {
      return current.some(index=> (currentPosition + index) % width === 0)
    }
    
    function checkRotatedPosition(P){
      P = P || currentPosition       //get current position.  Then, check if the piece is near the left side.
      if ((P+1) % width < 4) {         //add 1 because the position index can be 1 less than where the piece is (with how they are indexed).     
        if (isAtRight()){            //use actual position to check if it's flipped over to right side
          currentPosition += 1    //if so, add one to wrap it back around
          checkRotatedPosition(P) //check again.  Pass position from start, since long block might need to move more.
          }
      }
      else if (P % width > 5) {
        if (isAtLeft()){
          currentPosition -= 1
        checkRotatedPosition(P)
        }
      }
    }
    
    //rotate the tetromino
    function rotate() {
      undraw()
      currentRotation ++
      if(currentRotation === current.length) { //if the current rotation gets to 4, make it go back to 0
        currentRotation = 0
      }
      current = theTetrominoes[random][currentRotation]
      checkRotatedPosition()
      draw()
    }
    /////////
  
    
    
    //show up-next tetromino in mini-grid display
    const displaySquares = document.querySelectorAll('.mini-grid div')
    const displayWidth = 4
    const displayIndex = 0
  
  
    //the Tetrominos without rotations
    const upNextTetrominoes = [
      [1, displayWidth+1, displayWidth*2+1, 2], //lTetromino
      [0, displayWidth, displayWidth+1, displayWidth*2+1], //zTetromino
      [1, displayWidth, displayWidth+1, displayWidth+2], //tTetromino
      [0, 1, displayWidth, displayWidth+1], //oTetromino
      [1, displayWidth+1, displayWidth*2+1, displayWidth*3+1] //iTetromino
    ]
  
    //display the shape in the mini-grid display
    function displayShape() {
      //remove any trace of a tetromino form the entire grid
      displaySquares.forEach(square => {
        square.classList.remove('tetromino')
        square.style.backgroundColor = ''
      })
      upNextTetrominoes[nextRandom].forEach( index => {
        displaySquares[displayIndex + index].classList.add('tetromino')
        displaySquares[displayIndex + index].style.backgroundColor = colors[nextRandom]
      })
    }
  
    //add functionality to the button
    startBtn.addEventListener('click', () => {
      if(playerName === null) {
        playerName = prompt("Name");
      }

      if(playerName.length < 1) {
        return
      } else {
        nameContainer.innerHTML = playerName;
      }
      audioBtn.paused ? audioBtn.play() : audioBtn.pause();
      if (timerId) {
        clearInterval(timerId)
        timerId = null
      } else {
        changeLevel()
        draw()
        timerId = setInterval(moveDown, speed)
        nextRandom = Math.floor(Math.random()*theTetrominoes.length)
        displayShape()
      }
    })

    restartBtn.addEventListener('click', () => {
      location.reload();
    })
  
    //add score
    function addScore() {
      for (let i = 0; i < 199; i +=width) {
        const row = [i, i+1, i+2, i+3, i+4, i+5, i+6, i+7, i+8, i+9]
  
        if(row.every(index => squares[index].classList.contains('taken'))) {
          score +=10
          changeLevel();
          scoreDisplay.innerHTML = score
          row.forEach(index => {
            squares[index].classList.remove('taken')
            squares[index].classList.remove('tetromino')
            squares[index].style.backgroundColor = ''
          })
          const squaresRemoved = squares.splice(i, width)
          squares = squaresRemoved.concat(squares)
          squares.forEach(cell => grid.appendChild(cell))
        }
      }
    }

    const saveScore = () => {
      let highestScore = localStorage.getItem('scoreBoard');
      if(highestScore) {
        highestScore = JSON.parse(highestScore);

        if(score > highestScore.score) {
          const newHighestScore = {
            player: playerName,
            score: score
          }

          localStorage.setItem('scoreboard', JSON.stringify(newHighestScore));
        }
  
      } else {
        localStorage.setItem('scoreBoard', JSON.stringify({
          player: playerName,
          score: score
        }))
      } 
    }
  
    //game over
    function gameOver() {
      if(current.some(index => squares[currentPosition + index].classList.contains('taken'))) {
        scoreDisplay.innerHTML = 'end'
        saveScore();
        alert("Game Over")
        location.reload()
        clearInterval(timerId)
      }
    }

    //Level up
    const changeLevel = () => {
            if(score < 50) {
                level.innerHTML = 1;
            }
            else if(score === 50) {
                clearInterval(timerId)
                level.innerHTML = 2;
                speed -= 100;
                console.log(speed)
                console.log(score)
                timerId = setInterval(moveDown, speed)
            }
            else if(score === 100){
                clearInterval(timerId)
                level.innerHTML = 3;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed)
            }
            else if(score === 150){
                clearInterval(timerId)
                level.innerHTML = 4;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
            else if(score === 200){
                clearInterval(timerId)
                level.innerHTML = 5;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
            else if(score === 250){
                clearInterval(timerId)
                level.innerHTML = 6;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
            else if(score === 300){
                clearInterval(timerId)
                level.innerHTML = 7;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
            else if(score === 400){
                clearInterval(timerId)
                level.innerHTML = 8;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
            else if(score === 450){
                clearInterval(timerId)
                level.innerHTML = 9;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
            else if(score === 500){
                clearInterval(timerId)
                level.innerHTML = 10;
                speed -= 100;
                console.log(speed)
                timerId = setInterval(moveDown, speed);
            }
        }
  
  })