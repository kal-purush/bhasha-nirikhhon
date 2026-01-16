function AI(grid) {
  this.grid = grid;
}

/*AI.prototype.evaluation = function () {
  var edgeScore = 0;
  if(this.gird.largestTileInEdge()) {
    edgeScore = 100;
  }
  var bonuses = this.grid.availableCells().length;
  return edgeScore + bonuses * 2;
};*/
AI.prototype.evaluation = function () {
  var bonuses = this.grid.availableCells().length;
  var edgeScore = 0;
  /*if(this.grid.largestTileInEdge()) {
    edgeScore = 100;
  }*/
  return this.grid.monotonicity() + bonuses + edgeScore + this.grid.smoothness();
};

//minimax search with alpha-beta pruning:
AI.prototype.minimax = function(alpha, beta, depth, player) {
  var bestEval;
  var bestMove = -1;
  //var directions = [0, 1, 2, 3];
  var result;
  if(this.grid.isGameTerminated()) {
    if(this.gird.hasWon()) {
      //bestEval = Infinity;
      return {eval: Infinity, move: direction};
    }
  }
    //bestEval = this.evaluation();
    //result = {eval: bestEval, move: direction};
  else if(depth == 0) {
    //bestEval = this.evaluation();
    return {eval: this.evaluation(), move: bestMove};
  }
  else {
    //player's turn:
    if(player == this.grid.playerTurn) {
      bestEval = alpha;
      for(var direction in [1, 2, 3, 4]) {
        var newGrid = this.grid.clone();
        if(newGrid.move(direction).moved) {
          var newAI = new AI(newGrid);
          /*if(depth == 0) {
            return {eval: newAI.evaluation(), move: direction};
          }*/
          //else {
            result = newAI.minimax(alpha, beta, depth-1, false);
          //}
          if(result.eval > alpha) {
            alpha = result.eval;
            bestMove = direction;
          }
          //alpha = Math.max(alpha, bestEval);
          // * β cut-off *
          if(beta <= alpha) {
            //result = {eval: beta, move: bestMove};
            //return result;
            break;
          }
        }
      }
      bestEval = alpha;
    }
    //computer's turn:
    else {
      bestEval = beta;
      //var list = [2, 4];
      //var worstScores = [];
      var possibleScores = [];
      var cells = this.grid.availableCells();
      //put 2 and 4 in each empty cell and evaluate the values and find the worst one:
        for(var val in [2, 4]) {
          for(var x in cells) {
            possibleScores.push(cells[x]);
          }
        }

        //for each worst case, perform the recursion:
        for(var j = 0; j < possibleScores.length; j++) {
          var newGrid2 = this.gird.clone();
          var AI2 = new AI(newGrid2);
          result = AI2.minimax(alpha, beta, depth-1, true);
          if(result.eval < beta) {
            beta = result.eval;
          }
        // * α cut-off *
        if(beta <= alpha) {
          //result = {eval: alpha, move: null};
          //return result;
          break;
        }
      }
      bestEval = beta;
      if(cells.length == 0) {
        bestEval = 0;
      }
    }
  }
  return {eval: bestEval, move: bestMove};
};


AI.prototype.nextMove = function() {
  return this.minimax(-Infinity, Infinity, 5, this.grid.playerTurn);
};