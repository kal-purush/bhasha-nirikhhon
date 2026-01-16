function Game() {
  this.width = 10,
    this.height = 10,
    this.furry = new Furry(0, 0),
    this.coin = new Coin,
    this.score = 0,
    this.board = document.querySelectorAll('#board div'),
    this.scoreboard = document.querySelector('#scoreboard'),
    self = this,
    this.handler = setInterval(this.tick, 500),
    document.addEventListener("keydown", this.keyboard)
};

function Furry(x, y) {
  this.x = x,
    this.y = y,
    this.direction = "right"
};

function Coin() {
  this.x = Math.floor(10 * Math.random()),
    this.y = Math.floor(10 * Math.random())
};

Game.prototype.position = function(a, b) {
  return a + 10 * b;
};

Game.prototype.render = function() {

  var furryPosition = this.position(this.furry.x, this.furry.y);
  var coinPosition = this.position(this.coin.x, this.coin.y);

  for(var i = 0; i < this.board.length; i++) {
    this.board[i].classList.remove('furry');
  };

  if((self.furry.y >= 0) && (self.furry.y < 10) && (self.furry.x >= 0) && (self.furry.x < 10)) {
    self.board[furryPosition].classList.add("furry");

  } else {
    document.querySelector('#board').classList.add("hide");
    document.querySelector('body').classList.add("red");
    document.getElementById('over').style.display = "block";
    var again = document.getElementById('again');

    again.style.display = "inline-block";
    again.style.border = "1px solid black";
    again.addEventListener("click", function onClick(e) {

      window.location.reload(true);
    })
    clearInterval(self.handler);
  };
  self.board[coinPosition].classList.add('coin');


};



Game.prototype.keyboard = function(event) {
  key = event.which;
  switch(key) {
    case 37:
      self.furry.direction = "left";

      break;
    case 38:
      self.furry.direction = "up";

      break;
    case 39:
      self.furry.direction = "right";

      break;
    case 40:
      self.furry.direction = "down";

      break;


  }

};

Game.prototype.tick = function() {


  switch(self.furry.direction) {
    case "right":
      self.furry.x++;
      break;
    case "left":
      self.furry.x--;
      break;
    case "down":
      self.furry.y++;
      break;
    case "up":
      self.furry.y--;
      break;
  }
  self.render();

  var furryPosition = self.position(self.furry.x, self.furry.y);
  var coinPosition = self.position(self.coin.x, self.coin.y);

  if(furryPosition == coinPosition) {
    self.score++;
    self.board[furryPosition].classList.remove('coin');
    self.coin = new Coin;
    self.board[self.position(self.coin.x, self.coin.y)].classList.add('coin');
    self.scoreboard.innerHTML = self.score;


  }

}

document.addEventListener("DOMContentLoaded", function() {
  new Game;
})