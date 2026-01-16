var GoDrawerEnv = function(size, ctx) {
  this.size = size;
  this.ctx  = ctx;

  this.paddingTop    = 5;
  this.paddingBottom = 30;
  this.paddingLeft   = 10;
  this.paddingRight  = 10;

  this.resize = function(width, height) {
    this.canvasWidth  = width;
    this.canvasHeight = height;

    this.boardAreaWidth  = this.canvasWidth  - this.paddingLeft - this.paddingRight;
    this.boardAreaHeight = this.canvasHeight - this.paddingTop  - this.paddingBottom;
    this.hgrid = Math.floor(Math.min(this.boardAreaWidth, this.boardAreaHeight) / (2 * size));
    this.grid  = this.hgrid * 2;
    this.boardSize = this.grid * size;

    this.xOffset = (this.boardAreaWidth  - this.boardSize) / 2 + this.paddingLeft;
    this.yOffset = (this.boardAreaHeight - this.boardSize) / 2 + this.paddingTop;

    ctx.canvas.width  = this.canvasWidth;
    ctx.canvas.height = this.canvasHeight;
  }

  this.drawLine = function(x1, y1, x2, y2) {
    ctx.beginPath();
    ctx.moveTo(x1 + 0.5, y1 + 0.5);
    ctx.lineTo(x2 + 0.5, y2 + 0.5);
    ctx.stroke();
  }

  this.drawCircle = function(x, y, r, fill) {
    ctx.beginPath();
    ctx.arc(x + 0.5, y + 0.5, r, 0, Math.PI*2, false);
    if (fill)
      ctx.fill();
    else
      ctx.stroke();
  }

  this.drawImage = function(image, x, y, width, height) {
    try {
      ctx.drawImage(image, x + 0.5, y + 0.5, width, height);
    } catch (e) {
      if (e.name != "NS_ERROR_NOT_AVAILABLE")
      throw e;
    }
  }

  this.toX = function(i) {
    return this.grid * i + this.hgrid + this.xOffset;
  }

  this.toY = function(j) {
    return this.grid * j + this.hgrid + this.yOffset;
  }

  this.toI = function(x) {
    return Math.floor((x - this.xOffset) / this.grid);
  }

  this.toJ = function(y) {
    return Math.floor((y - this.yOffset) / this.grid);
  }

  this.mouseEventToIJ= function(e) {
    var rect = e.target.getBoundingClientRect();
    var x = e.clientX - rect.left;
    var y = e.clientY - rect.top;
    return { i: this.toI(x), j: this.toJ(y) };
  }
}

var GoDrawer = function(size, ctx) {
  var offscreen = document.createElement('canvas');

  this.env = new GoDrawerEnv(size, offscreen.getContext('2d'));
  this.handlers = [];

  this.addDrawHandler = function(handler) {
    this.handlers.push(handler);
  }

  this.resize = function(width, height) {
    offscreen.width  = width;
    offscreen.height = height;

    this.env.resize(width, height);
    this.draw();
  }

  this.draw = function() {
    for (var i = 0; i < this.handlers.length; i++) {
      var handler = this.handlers[i];
      if (handler.draw) {
        ctx.save();
        handler.draw(this.env);
        ctx.restore();
      }
    }
    ctx.drawImage(offscreen, 0, 0);
  }
}

var DefaultGoDrawerFactory = function() {

  var BackgroundDrawer = function(option) {
    var bgColor  = option.shouldGet("bgColor", "white");
    var boardImg = option.shouldGet("boardImg");

    this.draw = function(env) {
      env.ctx.fillStyle = bgColor;
      env.ctx.fillRect(0, 0, env.canvasWidth, env.canvasHeight);
      env.ctx.drawImage(boardImg, env.xOffset, env.yOffset, env.boardSize, env.boardSize);
    }
  }

  var GridDrawer = function(option) {
    var lineWidth = option.shouldGet("lineWidth", 1);
    var lineColor = option.shouldGet("lineColor", "black");

    this.draw = function(env) {
      this.drawLine(env);
      this.drawPoint(env);
    }

    this.drawLine = function(env) {
      var size = env.size;
      env.ctx.lineWidth   = Number(lineWidth);
      env.ctx.strokeStyle = lineColor;
      for (var i = 0; i < size; i++) {
        env.drawLine(env.toX(0), env.toY(i), env.toX(size - 1), env.toY(i));
        env.drawLine(env.toX(i), env.toY(0), env.toX(i), env.toY(size - 1));
      }
    }

    this.drawPoint = function(env) {
      var size = env.size;
      var lpos = 3;
      var rpos = size - lpos - 1;
      var hpos = Math.floor(size / 2);

      var points = [];
      if (size > 10) {
        points.push([lpos, lpos]);
        points.push([rpos, lpos]);
        points.push([lpos, rpos]);
        points.push([rpos, rpos]);
      }
      if ((size % 2 == 1) && size >= 19) {
        points.push([hpos, hpos]);
        points.push([lpos, hpos]);
        points.push([hpos, lpos]);
        points.push([rpos, hpos]);
        points.push([hpos, rpos]);
      }

      var pointSize = env.hgrid / 4;
      env.ctx.fillStyle = 'black';
      for (var i = 0; i < points.length; i++) {
        var p = points[i];
        env.drawCircle(env.toX(p[0]), env.toY(p[1]), pointSize, true);
      }
    }
  }

  var StoneDrawer = function(option, player) {
    var blackStoneColor = option.shouldGet("blackStoneColor", "black");
    var whiteStoneColor = option.shouldGet("whiteStoneColor", "white");
    var blackStoneImg   = option.shouldGet("blackStoneImg");
    var whiteStoneImg   = option.shouldGet("whiteStoneImg");

    this.draw = function(env) {
      this.drawShadow(env);
      this.drawCircle(env);
      this.drawImage(env);
    }

    this.drawShadow = function(env) {
      var size = env.size;
      var offset = env.hgrid / 6;
      var shadowSize = env.hgrid * 0.9;
      var ctx = env.ctx;
      ctx.fillStyle = 'rgba(0, 0, 0, 0.2)';
      for (var i = 0; i < size; i++)
        for (var j = 0; j < size; j++)
          if (StoneType.exists(player.getStone(i, j)))
            env.drawCircle(env.toX(i) - offset, env.toY(j) + offset, shadowSize, true);

    }

    this.drawCircle = function(env) {
      var size = env.size;
      var stoneSize = env.hgrid - 1;
      var ctx = env.ctx;
      ctx.globalAlpha = 1.0;

      for (var i = 0; i < size; i++) {
        for (var j = 0; j < size; j++) {
          var stone = player.getStone(i, j);
          if (StoneType.exists(stone)) {
            var color = stone == StoneType.BLACK ? blackStoneColor : whiteStoneColor;
            ctx.fillStyle = color;
            ctx.strokeStyle = 'rgb(0, 0, 0)';
            ctx.lineWidth = 1;
            env.drawCircle(env.toX(i), env.toY(j), stoneSize, true);
          }
        }
      }
    }

    this.drawImage = function(env) {
      var size = env.size;
      var imgSize = env.grid - 2;
      for (var i = 0; i < size; i++) {
        for (var j = 0; j < size; j++) {
          var stone = player.getStone(i, j);
          if (StoneType.exists(stone)) {
            var img = stone == StoneType.BLACK ? blackStoneImg : whiteStoneImg;
            env.drawImage(img, env.toX(i) - imgSize / 2, env.toY(j) - imgSize / 2, imgSize, imgSize);
          }
        }
      }
    }
  }

  var HamaDrawer = function(option, player) {
    this.draw = function(env) {
      if (env.paddingBottom < 20)
        return;
      var ctx = env.ctx;
      var y = env.paddingTop + env.boardAreaHeight + 15;
      var basex = env.xOffset + 5;
      ctx.fillStyle = "black";
      ctx.font = "14pt Arial";
      ctx.textAlign = "left";
      ctx.textBaseline = "middle";
      ctx.fillText(player.rule.getBlackHama(), basex + 60, y);
      ctx.fillText(player.rule.getWhiteHama(), basex + 140, y);

      ctx.strokeStyle = "black";
      ctx.fillStyle = "black";
      env.drawCircle(basex + 40, y, 10, true);
      ctx.fillStyle = "white";
      env.drawCircle(basex + 120, y, 10, true);
      env.drawCircle(basex + 120, y, 10, false);

      ctx.fillStyle = "black";
      var move = player.propUtil.getMoveFrom(player.sgfTree.current);
      if (move && move.isPass())
        ctx.fillText((move.stone == StoneType.BLACK ? "Black" : "White") + " Pass", basex + 180, y);
    }
  }

  var ChildrenDrawer = function(option, player) {
    var branchPointColor = option.shouldGet("branchPointColor", "pink");
    this.draw = function(env) {
      var tree = player.sgfTree;
      var nodes = tree.current.children;
      if (nodes.length < 2)
        return;
      var pointSize = env.hgrid / 2;
      env.ctx.fillStyle = branchPointColor;
      var stone = player.rule.getNextStone();
      for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i];
        var move = player.propUtil.getMoveFrom(node);
        if (move && move.stone == stone)
          env.drawCircle(env.toX(move.x), env.toY(move.y), pointSize, true);
      }
    }
  }


  var NumberDrawer = function(option, player) {
    var size = player.size;
    var numberTable = new Array(size);
    for (var i = 0; i < size; i++)
      numberTable[i] = new Array(size);

    this.draw = function(env) {
      if (!option.shouldGet("stoneNumber", false))
        return;
      var nodes = player.sgfTree.toSequence();
      var ctx = env.ctx;
      ctx.font = Math.floor(env.grid * 0.5) + "px Arial";
      ctx.textAlign = "center";
      ctx.textBaseline = "middle";
      var cnt = 0;

      for (var i = 0; i < size; i++)
        for (var j = 0; j < size; j++)
          numberTable[i][j] = undefined;

      for (var i = 0; i < nodes.length; i++) {
        var node = nodes[i];
        var move = player.propUtil.getMoveFrom(node);
        if (move) {
          cnt += 1;
          if (StoneType.exists(player.getStone(move.x, move.y)))
            numberTable[move.x][move.y] = cnt;
        }
      }

      for (var i = 0; i < size; i++) {
        for (var j = 0; j < size; j++) {
          var cnt = numberTable[i][j];
          if (cnt !== undefined) {
            ctx.fillStyle = player.getStone(i, j) == StoneType.BLACK ? "white" : "black";
            ctx.fillText(cnt.toString(), env.toX(i), env.toY(j));
          }
        }
      }
    }
  }

  function createImage(drawer, path) {
    var img = new Image();
    img.src = path;
    img.onload = function() {
      drawer.draw();
    }
    return img;
  }

  this.create = function(player, ctx, option) {
    var drawer = new GoDrawer(player.size, ctx);
    drawer.resize(ctx.canvas.width, ctx.canvas.height);

    option.blackStoneImg = createImage(drawer, option.blackStoneImagePath);
    option.whiteStoneImg = createImage(drawer, option.whiteStoneImagePath);
    option.boardImg      = createImage(drawer, option.boardImagePath);

    option.shouldGet = function(key, defaultVal) {
      var x = this[key];
      if (x !== undefined)
        return x;
      else if(defaultVal !== undefined)
        return defaultVal;

      throw new Error("shouldGet failed: " + key);
    }

    drawer.addDrawHandler(new BackgroundDrawer(option));
    drawer.addDrawHandler(new GridDrawer(option));
    drawer.addDrawHandler(new StoneDrawer(option, player));
    drawer.addDrawHandler(new HamaDrawer(option, player));
    drawer.addDrawHandler(new ChildrenDrawer(option, player));
    drawer.addDrawHandler(new NumberDrawer(option, player));

    return drawer;
  }
}

var TreeDrawer = function(player, treeCanvas, ctx) {
  var transX = 0;
  var transY = 0;
  var saveX  = null;
  var saveY  = null;
  this.matrix = null;
  this.currentX = null;
  this.currentY = null;
  this.r = 8;

  function drawLine(x1, y1, x2, y2) {
    ctx.beginPath();
    ctx.moveTo(x1, y1);
    ctx.lineTo(x2, y2);
    ctx.stroke();
  }

  function drawCircle(x, y, r, fill) {
    ctx.beginPath();
    ctx.arc(x, y, r, 0, Math.PI*2, false);
    if (fill)
      ctx.fill();
    else
      ctx.stroke();
  }

  function makeMatrix() {
    var matrix = new Array();
    makeMatrixHelper(matrix, player.sgfTree.getRoot(), 0, 0);
    return matrix;
  }

  function makeMatrixHelper(matrix, node, x, y) {
    if (!matrix[x])
      matrix[x] = new Array();

    matrix[x][y] = node;

    var height = 0;
    for (var j = 0; j < node.children.length; j++)
      height += makeMatrixHelper(matrix, node.children[j], x + 1, y + height);

    return node.children.length == 0 ? 1 : height;
  }

  function toX(i) {
    return i * 20 + 50;
  }

  function toY(j) {
    return j * 20 + 50;
  }

  function toI(x) {
    return Math.round((x - transX - 50) / 20);
  }

  function toJ(y) {
    return Math.round((y - transY - 50) / 20);
  }

  this.resetPosition = function() {
    if (saveX == null && saveY == null && this.currentX && this.currentY) {
      transX = -this.currentX + 100;
      transY = -this.currentY + 100;
    }
  }

  this.draw = function(dontReset) {
    ctx.fillStyle = '#fcf0da';
    ctx.fillRect(0, 0, 600, 600);

    this.matrix = makeMatrix();
    var matrix = this.matrix;

    var lines = [];
    var circles = [];
    
    for (var i = 0; i < matrix.length; i++) {
      for (var j = 0; j < matrix[i].length; j++) {
        var node = matrix[i][j];
        if (node && matrix[i+1]) {
          var prevK = j;
          for (var k = j; k < matrix[i+1].length && (matrix[i][k] === node || !matrix[i][k]); k++) {
            if (matrix[i+1][k]) {
              lines.push([toX(i), toY(k), toX(i + 1), toY(k)]);
              if (prevK != k) {
                lines.push([toX(i), toY(prevK), toX(i), toY(k)]);
                prevK = k;
              }
            }
          }
        }
      }
    }

    for (var i = 0; i < matrix.length; i++) {
      for (var j = 0; j < matrix[i].length; j++) {
        var node = matrix[i][j];
        if (node) {
          var move = player.propUtil.getMoveFrom(node);
          var color = 'green';
          if (move)
            color = (move.stone == StoneType.BLACK) ? 'black' : 'white';

          var x = toX(i);
          var y = toY(j);

          circles.push([color, x, y, this.r, true, 0]);
          circles.push(['black', x, y, this.r, false, 1]);
          if (node === player.sgfTree.current) {
            circles.push(['red', x, y, this.r, false, 2]);
            this.currentX = x;
            this.currentY = y;
          }
        }
      }
    }
    if (!dontReset)
      this.resetPosition();
    ctx.save();
    ctx.setTransform(1, 0, 0, 1, transX, transY);
    ctx.strokeStyle = 'black';
    for (var i = 0; i < lines.length; i++) {
      var a = lines[i];
      drawLine(a[0], a[1], a[2], a[3]);
    }

    for  (var i = 0; i < circles.length; i++) {
      var a = circles[i];
      ctx.lineWidth = a[5];
      ctx.strokeStyle = a[0];
      ctx.fillStyle   = a[0];
      drawCircle(a[1], a[2], a[3], a[4]);
    }

    ctx.font = "10px Arial";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";
    ctx.fillStyle = 'black';
    for (var i = 1; i < this.matrix.length; i += 5) {
      ctx.fillText(i, toX(i), toY(-1));
    }
    ctx.restore();
  }

  var varThis = this;
  function handleDownEvent(p) {
    saveX = p.x;
    saveY = p.y;
  }

  function handleMoveEvent(p) {
    var x = p.x;
    var y = p.y;
    if (saveX !== null && saveY !== null) {
      transX += x - saveX;
      transY += y - saveY;
      saveX = x;
      saveY = y;
    }
    varThis.draw(true);
  }

  function handleUpEvent(p) {
    saveX = null;
    saveY = null;
    
    var x = p.x;
    var y = p.y;

    var i = toI(x);
    var j = toJ(y);
    var node = null;
    if (varThis.matrix[i])
      node = varThis.matrix[i][j];
    if (node) {
      player.setCurrent(node);
    }

    varThis.draw(true);
  }

  function windowToCanvas(x, y) {
    var bbox = treeCanvas.getBoundingClientRect();
    return { x: (x - bbox.left) * (treeCanvas.width / bbox.width),
             y: (y - bbox.top)  * (treeCanvas.height / bbox.height)
           }
  }

  treeCanvas.addEventListener("mouseout", function(e) {
    saveX = null;
    saveY = null;
    varThis.draw(true);
  });

  treeCanvas.addEventListener("touchstart", function(e) {
    e.preventDefault(e);
    if (e.changedTouches.length == 1) {
      var t = e.changedTouches[0];
      handleDownEvent(windowToCanvas(t.clientX, t.clientY));
    }
  });

  treeCanvas.addEventListener("touchmove", function(e) {
    e.preventDefault(e);
    if (e.changedTouches.length == 1) {
      var t = e.changedTouches[0];
      handleMoveEvent(windowToCanvas(t.clientX, t.clientY));
    }
  });

  treeCanvas.addEventListener("touchend", function(e) {
    e.preventDefault(e);
    if (e.changedTouches.length == 1) {
      var t = e.changedTouches[0];
      handleUpEvent(windowToCanvas(t.clientX, t.clientY));
    }
  });

  treeCanvas.addEventListener("mousedown", function(e) {
    e.preventDefault(e);
    handleDownEvent(windowToCanvas(e.clientX, e.clientY));
  });

  treeCanvas.addEventListener("mousemove", function(e) {
    e.preventDefault(e);
    handleMoveEvent(windowToCanvas(e.clientX, e.clientY));
  });

  treeCanvas.addEventListener("mouseup", function(e) {
    e.preventDefault(e);
    handleUpEvent(windowToCanvas(e.clientX, e.clientY));
  });

  this.draw();
  this.resetPosition();
}
