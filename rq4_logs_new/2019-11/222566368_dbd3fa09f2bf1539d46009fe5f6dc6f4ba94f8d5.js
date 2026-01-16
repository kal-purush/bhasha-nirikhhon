class Tetromino {
    
    constructor(piece, color, dropInterval, x,) {
      this.x = x;
      this.y = 0;
      this.color = color;
      this.piece = piece;
      this.position = 0;
      this.activePiece = this.piece[this.position];
      this.dropTime = Date.now();
      this.dropInterval = dropInterval;
      this.locked = false;
    }
  
    draw(context, x = this.x, y = this.y) {
    for(let r = 0; r < this.activePiece.length; r++) {
      for (let c = 0; c < this.activePiece[r].length; c++) {
        if(this.activePiece[r][c] !== 0){
          drawSquare(context, x + c, y + r, this.color,);
        }
      }
    }
   }
    
    rotate(){
      let kick = 0;
      let rotatedPiece = this.piece[(this.position + 1) % this.piece.length]
      if(this.collision(0, 0, rotatedPiece)){
        if(this.x > columns / 2){
          kick = -1;
        } else {
          kick = 1;
        }
      }
      // check collision after kick 
     if(!this.collision(kick, 0, rotatedPiece)){
       this.x += kick;
       this.position = (this.position + 1) % this.piece.length;
       this.activePiece = this.piece[this.position];
       if(soundOn){
        rotateSound.play();
       }
     }
    }
    
    dropDown(){
      if(Date.now() - this.dropTime >= this.dropInterval){
        if(!this.collision(0, 1, this.activePiece)){
          this.y++;
        } else {
          this.lockPiece();
        }
        this.dropTime = Date.now();
      }
    }
    
    moveDown(){
      if(this.collision(0, 1, this.activePiece) !== true ){
        this.y++;
        score += 1;
      } 
    }
    
    moveLeft(){
     if(this.collision(-1, 0, this.activePiece) !== true ){
         this.x--;
     }
    }
    
    moveRight(){
      if(this.collision(1, 0, this.activePiece) !== true ){
        this.x++;
      }
    }
    
    collision(x, y, piece){
     for(let r = 0; r < piece.length; r++) {
      for (let c = 0; c < piece[r].length; c++) {
        if(piece[r][c] === 0){
          continue;
        }
        let newX = this.x + c + x;
        let newY = this.y + r + y;
        if(newX < 0 || newX >= columns || newY >= rows){
          return true;
        }
        if(newY < 0){
            continue;
          }
        if(gameBoard[newY][newX] !== empty){
          return true;
        }
      }
    }
      return false;
   }
    
    lockPiece(){
      this.locked = true;
      for(let r = 0; r < this.activePiece.length; r++) {
        for (let c = 0; c < this.activePiece[r].length; c++) {
          if(this.activePiece[r][c] === 0){
            continue; 
          } 
          gameBoard[this.y + r][this.x + c] = this.color;
        }
      }
      if(soundOn){
        lockPieceSound.play();
      }
    }

    isOverflowed(){
      if(this.y === 0 && this.locked){
        return true
      }
      return false
    }
  }