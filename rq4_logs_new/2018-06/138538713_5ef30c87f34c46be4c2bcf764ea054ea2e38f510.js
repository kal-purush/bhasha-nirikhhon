window.onload = function() {
    
//canvas
var canvas = document.getElementById('canvas');
var ctx = canvas.getContext('2d');


//constants
var interval;
var frames = 0;
var images = {
    xolo: './images/xoloitzcuintle.png',
    bg: './images/cdmx3.png',
    car: './images/taxi.png'
}
var sound = new Audio();
sound.src = './audio/tamales.mp3';
sound.loop = true;
var cars = [];
var trucks = [];

//classes
class Board{
    constructor(){
        this.x = 0;
        this.y = 0;
        this.width = canvas.width;
        this.height = canvas.height;
        this.color = '#a9a9a9'
        this.image = new Image();
        this.image.src = images.bg;
        this.image.onload = function(){
            this.draw();
        }.bind(this)
    }
  
    // gameOver(){
    //     ctx.font = "80px Avenir";
    //     ctx.fillText("Game Over", 40,100);
    //     ctx.font = "20px Avenir";
    //     ctx.fillStyle = 'yellow';
    //     var playerScore = Math.floor(frames / 60);
    //     ctx.fillText("Press 'Esc' to reset", 170,130);
    //     ctx.fillStyle = 'red';
    //     ctx.font = "40px Avenir";
    //     ctx.fillText("Player Score :" + playerScore, 120,200)
    // }
  
    draw(){
        ctx.fillRect = this.color;
        ctx.drawImage(this.image, this.x,this.y,this.width,this.height);
        ctx.beginPath();
        ctx.strokeStyle = 'yellow';
        ctx.lineWidth = '8';
        ctx.setLineDash([15, 20]);
        ctx.moveTo(0,canvas.height - 128);
        ctx.lineTo(canvas.width,canvas.height - 128);
        ctx.closePath();
        ctx.stroke();
        ctx.beginPath();
        ctx.setLineDash([15, 20]);
        ctx.moveTo(0,canvas.height - 192);
        ctx.lineTo(canvas.width,canvas.height - 192);
        ctx.closePath();
        ctx.stroke();
        ctx.fillStyle = '#eaf7ff';
        ctx.fillRect = (0,110,canvas.width,256);
    }
}
  
class Xolo{
  constructor(){
      this.x = canvas.width / 2;
      this.y = canvas.height - 60;
      this.width = 58;
      this.height = 58;
      this.image = new Image();
      this.image.src = images.xolo;
      this.image.onload = function(){
          this.draw();
      }.bind(this);

  }

//   rise(){
//       this.y-=25;
//   }

//   isTouching(item){
//       return  (this.x < item.x + item.width) &&
//               (this.x + this.width > item.x) &&
//               (this.y < item.y + item.height) &&
//               (this.y + this.height > item.y);
//     }
  

    draw(){
      ctx.drawImage(this.image,this.x,this.y,this.width,this.height);
    }

    goLeft(){
        if (this.x >= this.width) this.x -=64;
    }
  
    goRight(){
        if (this.x < canvas.width - 64) this.x +=64;
    }

    goUp(){
        if (this.y >= 64) this.y -=64;
    }

    goDown(){
        if (this.y < canvas.height - 64) this.y += 64;
    }

}

class Car{
    constructor(){
        this.x = canvas.width / 2;
        this.y = canvas.height - 128;
        this.width = 64;
        this.height = 64;
        this.image = new Image();
        this.image.src = images.car;
        this.image.onload = function(){
            this.draw();
        }.bind(this);
  
    }
  
  //   rise(){
  //       this.y-=25;
  //   }
  
  //   isTouching(item){
  //       return  (this.x < item.x + item.width) &&
  //               (this.x + this.width > item.x) &&
  //               (this.y < item.y + item.height) &&
  //               (this.y + this.height > item.y);
  //     }
    
  
      draw(){
        ctx.drawImage(this.image,this.x,this.y,this.width,this.height);
      }
  }


//instances
var board = new Board();
var xolo = new Xolo();
var taxi = new Car();

//main functions
function update(){
    frames++;
    ctx.clearRect(0,0,canvas.width,canvas.height);
    board.draw();
    xolo.draw();
    taxi.draw();
    // generatePipes();
    // drawPipes();
  }
  
  function start(){
    interval = setInterval(update, 1000/60);
    sound.play();
  }
  
//event listeners



addEventListener('keydown', function(e){
    switch(e.keyCode){      
        case 39:
        // if(xolo.x === canvas.width - xolo.width) return;
            xolo.goRight();
            break;
        case 37:
            // if(xolo.x === 0) return;
            xolo.goLeft();
            break;
        case 38:
            xolo.goUp();
            // start();
            break;
        case 40:
            xolo.goDown();
            break;
    }

})  

}//end