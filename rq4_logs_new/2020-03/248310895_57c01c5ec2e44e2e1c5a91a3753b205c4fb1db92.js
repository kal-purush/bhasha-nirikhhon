(function() {
  // Некоторые констатны,которые настраивают игру
  var FPS = 40;
  var W = 600;
  var H = 600;                

  var SNAKE_CHUNK_RADIUS = 3;
  var COLLISION_SENSIBILITY_RADIUS = 3;
  var UNCOLLIDABLE_SNAKE_HEAD_LENGTH = 3;
  var FOOD_RADIUS = 5;
  ``;
  var INITIAL_SNAKE_LENGTH = 30;
  var SNAKE_CHUNK_GAP = 3;
  var INITIAL_VELOCITY = 2;
  var ANGULAR_SPEED = Math.PI * 1.5;
  var NEW_CHUNKS_PER_DINNER = 3;
  var FOOD_INCREASE_STEP = 5;
  var VELOCITY_INCREASE = 0.3;

  var KEY_LEFT = 37;
  var KEY_RIGHT = 39;

  // HTML5 canvas in~`nce and its 2D graphics context
  var canvas = document.getElementById("canvas");
  var left = document.getElementById("left");

  var right = document.getElementById("right");
  var ctx = canvas.getContext("2d");

  // Текущее состояние игры
  // (that means, whether it is being played or we see menu or someting)
  var state;

  // Game state prototype
  var GameState = {
    update: function() {},
    draw: function() {},
    mouseDown: function() {},
    keyDown: function() {},
    keyUp: function() {}
  };

  /*
       Конструктор принимает три аргумента: координата x, y
 координата и угол, определяющий направление
    */
  var SnakeChunk = function(x, y, angle) {
    this.x = x;
    this.y = y;
    this.angle = angle;

    this.getPosition = function() {
      return {
        x: this.x,
        y: this.y,
        angle: this.angle
      };
    };

    this.setPosition = function(newPos) {
      this.x = newPos.x;
      this.y = newPos.y;
      this.angle = newPos.angle;
    };

    /*
          Аргумент "isHead", это означает, что если кусок является головой змеи,
 один говорит, что функция обновления, какой из двух сценариев связано 
 она должна подчиняться. Значение следующего аргумента зависит исключительно от
 предыдущий: если кусок является головой, "param" - скорость змеи; если это
 нет, тогда это позиция и направление предыдущего блока.
 Аргумент chunks представляет собой список всех chunks, необходимых для коллизии
 обнаружение, и если столкновение было обнаружено, 'collisionCallback'
 будет вызван.
        */
    this.update = function(isHead, param, chunks, collisionCallback) {
      // Сохраните старый " context" (положение и направление), чтобы передать его
      // к следующему куску
      var pos = this.getPosition();

      if (isHead) {
        // Если это голова, то это не хвост. Но более того
        // важно, 'param' - скорость змеи
        var velocity = param;

        // Здесь мы добавляем вектор скорости к вектору положения
        this.x += velocity * Math.cos(this.angle);
        this.y += velocity * Math.sin(this.angle);

        // Проверка, не разбилась ли змея  внезапно
        // столкновение со стеной
        if (this.x >= W || this.x < 0 || this.y >= H || this.y < 0) {
          collisionCallback();
        }

        // Также, мы игнорируем первый UNCOLLIDABLE_SNAKE_HEAD_LENGTH куски
        // чтобы не закончить игру из-за невинного изменения направления
        for (
          var i = 0;
          i < chunks.length - UNCOLLIDABLE_SNAKE_HEAD_LENGTH;
          i++
        ) {
          if (
            Math.abs(this.x - chunks[i].x) < COLLISION_SENSIBILITY_RADIUS &&
            Math.abs(this.y - chunks[i].y) < COLLISION_SENSIBILITY_RADIUS
          ) {
            collisionCallback();
          }
        }
      } else {
        // необходимо получить позицию предыдущего блока
        var newPos = param;
        this.setPosition(newPos);
      }

      return pos;
    };

    this.draw = function() {
      ctx.fillStyle = "black";
      ctx.beginPath();
      ctx.arc(this.x, this.y, SNAKE_CHUNK_RADIUS, 0, Math.PI * 2);
      ctx.fill();
      ctx.closePath();
    };
  };

  /*
       Этот класс описывает food
    */
  var Food = function() {
    var generateCoordinate = function(maximum) {
      return Math.floor(Math.random() * maximum);
    };

    this.newPosition = function() {
      this.x = generateCoordinate(W);
      this.y = generateCoordinate(H);
    };

    /*
       Берем змеиную голову, проверяем на столкновение
 с self, и, вероятно, вызвать соответствующий обратный вызов
        */
    this.update = function(snakeHead, eatenCallback) {
      if (
        Math.abs(snakeHead.x - this.x) < FOOD_RADIUS &&
        Math.abs(snakeHead.y - this.y) < FOOD_RADIUS
      ) {
        this.newPosition();
        eatenCallback();
      }
    };

    this.draw = function() {
      ctx.fillStyle = "red";
      ctx.beginPath();
      ctx.arc(this.x, this.y, FOOD_RADIUS, 0, Math.PI * 2);
      ctx.fill();
      ctx.closePath();
    };

    // Сгенерировать случайную позицию
    this.newPosition();
  };

  var normalizeTime = function(time) {
    var seconds = (time / 1000) | 0;
    var minutes = (seconds / 60) | 0;
    seconds %= 60;
    return `${(minutes < 10 ? "0" : "") + minutes}:${(seconds < 10 ? "0" : "") +
      seconds}`;
  };

  /*
        Класс игры
    */
  var Game = function() {
    this.chunks = [];
    this.food = [];
    this.velocity = INITIAL_VELOCITY;
    this.points = 0; // player's score
    this.time = 0;
    this.isTurningLeft = false;
    this.isTurningRight = false;

    // Cоздание змеи...
    for (var i = 0; i < INITIAL_SNAKE_LENGTH; i++) {
      this.chunks.push(
        new SnakeChunk(W / 2, H / 2 - SNAKE_CHUNK_GAP * i, -Math.PI / 2)
      );
    }

    // ...и что он должен есть
    this.food.push(new Food());

    this.draw = function() {
      ctx.fillStyle = "#ffffff";
      ctx.fillRect(0, 0, W, H);

      this.chunks.forEach(function(chunk) {
        chunk.draw();
      });

      this.food.forEach(function(foodItem) {
        foodItem.draw();
      });

      ctx.font = "40px VT323";
      ctx.fillStyle = "rgba(0, 0, 0, 0.8)";
      ctx.textBaseline = "top";
      ctx.textAlign = "right";
      ctx.fillText(this.points, W, 0);

      ctx.font = "20px Calligraffitti";
      ctx.fillStyle = "rgba(0, 0, 0, 0.8)";
      ctx.textBaseline = "top";
      ctx.textAlign = "left";

      ctx.fillText(normalizeTime(this.time), 0, 0);
    };

    this.update = function() {
      this.time += 1000 / FPS;
      // Если нажата левая или правая клавиша, поверните змеиную голову
      if (this.isTurningLeft) {
        this.chunks[this.chunks.length - 1].angle -= ANGULAR_SPEED / FPS;
      }
      if (this.isTurningRight) {
        this.chunks[this.chunks.length - 1].angle += ANGULAR_SPEED / FPS;
      }

      var self = this;

      // Обновить голову и получить ее предыдущее положение
      var pos = this.chunks[this.chunks.length - 1].update(
        true,
        this.velocity,
        this.chunks,
        function() {
          switchState(new GameOver(self.points, self.time));
        }
      );
      // Теперь обновим остальные куски, передав им новые позиции
      for (var i = this.chunks.length - 2; i >= 0; i--) {
        pos = this.chunks[i].update(false, pos);
      }

      // Обновление еды
      for (var i = 0; i < this.food.length; i++) {
        this.food[i].update(this.chunks[this.chunks.length - 1], function() {
          // Если что-то было съедено, увеличьте счет...
          self.points++;
          //и увеличить змею
          for (var j = 0; j < NEW_CHUNKS_PER_DINNER; j++) {
            self.chunks.unshift(
              new SnakeChunk(
                self.chunks[0].x,
                self.chunks[0].y,
                self.chunks[0].angle
              )
            );
          }
          // Теперь, если было достаточно cъедено еды, заполнить
          // игровое поле с новым экземпляром еды
          if (self.points % FOOD_INCREASE_STEP == 0) {
            self.food.push(new Food());
          }
          // И, наконец, увеличить скорость
          self.velocity += VELOCITY_INCREASE;
        });
      }
    };


    this.keyDownLeft = function(event) {
      this.isTurningLeft = true;
    };

    this.keyUpLeft = function(event) {
      this.isTurningLeft = false;
    };
    this.keyDownRight = function(event) {
      this.isTurningRight = true;
    };

    this.keyUpRight = function(event) {
      this.isTurningRight = false;
    };

    this.keyDown = function(event) {
      switch (event.keyCode) {
        case KEY_LEFT:
          this.isTurningLeft = true;
          break;
        case KEY_RIGHT:
          this.isTurningRight = true;
          break;
      }
    };

    this.keyUp = function(event) {
      switch (event.keyCode) {
        case KEY_LEFT:
          this.isTurningLeft = false;
          break;
        case KEY_RIGHT:
          this.isTurningRight = false;
          break;
      }
    };
  };
  Game.prototype = GameState;

  // The menu state
  var Menu = function() {
    this.draw = function() {
      ctx.fillStyle = "black";
      ctx.fillRect(0, 0, W, H);

      ctx.fillStyle = "white ";
      ctx.beginPath();
      ctx.moveTo(W / 2 - 50, H / 2 + 50);
      ctx.lineTo(W / 2 - 50, H / 2 + 150);
      ctx.lineTo(W / 2 + 50, H / 2 + 100);
      ctx.fill();
      ctx.closePath();

      ctx.font = "130px Faster One";
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillText("Snake", W / 2, 110, W);
      ctx.font = "23px Emilys Candy";
      ctx.fillText("🎯objective: Eat the red square", W / 2, 255, W);
      ctx.fillText("💀DONT hit the red borders or the snake body",W / 2, 290,W);
         };

    this.mouseDown = function(event) {
      switchState(new Game());
    };

    this.keyDown = function(event) {
      switchState(new Game());
    };
  };
  Menu.prototype = GameState;

  // The 'Game over' state
  var GameOver = function(score, time) {
    this.prototype = GameState;

    this.score = score;

    this.draw = function() {
      ctx.fillStyle = "rgba(225, 245, 220, 0.05)";
      ctx.fillRect(0, 0, W, H);

      ctx.fillStyle = "rgb(0, 0, 0)";
      ctx.font = "72px  Emilys Candy";
      ctx.textAlign = "center";
      ctx.textBaseline = "top";
      ctx.fillText("Your score: " + this.score, W / 2, 200, W);
      ctx.font = "18px Emilys Candy";
      ctx.fillText("Time: " + normalizeTime(time), W / 2, 290, W);
      ctx.font = "24px  Emilys Candy";
      ctx.fillText("Press any key to try again", W / 2, 320, W);
    };

    this.mouseDown = function(event) {
      switchState(new Game());
    };

    this.keyDown = function(event) {
      switchState(new Game());
    };
  };
  GameOver.prototype = GameState;

  // This function switches game states
  function switchState(newState) {
    state = newState;
    canvas.onmousedown = state.mouseDown;
    var i=0;
    left.onclick = function(event) { 
      if(i == 0)
      {
        state.keyDownLeft();
        i=1;
        return false;
      }
      if(i==1)
      {
        state.keyUpLeft();
        i=0;
        return false;
      } 
    };

    right.onclick = function(event) {
      if(i == 0)
      {
        state.keyDownRight();
        i=1;
        return false;
      }
      if(i==1)
      {
        state.keyUpRight();
        i=0;
        return false;
      } 
    };

    document.body.onkeydown = function(event) {
      state.keyDown(event);
      return false;
    };
    document.body.onkeyup = function(event) {
      state.keyUp(event);
      return false;
    };
  }

  switchState(new Menu());

  // Main game loop
  window.setInterval(function() {
    state.update();
    state.draw();
  }, 1000 / FPS);
})();