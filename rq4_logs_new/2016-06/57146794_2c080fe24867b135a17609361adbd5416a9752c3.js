$(document).on('ready',function () {
  var turn = 0;
  var winner = '';
  var winnerCells = [];

  var judge = function (player) {
    var my_cells = $('.' + player);
    for (var i = 0; i < my_cells.size(); i++) {
      if (my_cells[i] !== self){
        for (var j = 0; j < my_cells.size(); j++) {
          if (j !==i && my_cells[j]!== self && parseInt($(self).attr('hiddenattr'))+parseInt($(my_cells[i]).attr('hiddenattr'))+parseInt($(my_cells[j]).attr('hiddenattr')) === 15) {
            winner = player;
            winnerCells = Array(self, my_cells[i], my_cells[j])
            break;
            break;
          }
        }
      }
    }
    if (turn === 8 && $('.score').text() !== 'X WINS') {
      $('.score').text('DRAW');
      $('.game_over').fadeIn();
    }
  }

  var announcement = function (player) {
    if (winner) {
      $('.score').text(player+' WINS');
      $(winnerCells).css('color', 'tomato');
      // $(my_cells[i]).css('color', 'tomato');
      // $(my_cells[j]).css('color', 'tomato');
      $('td').off('click');
      $('.game_over').fadeIn();
    }
  }

  var bot = function () {
    var opponentCells = $('.X').toArray();
    var botCells = $('.O').toArray();
    var emptyCells = $('td');
    for (var i in opponentCells) {
      emptyCells = emptyCells.not(opponentCells[i]);
    }
    for (var j in botCells) {
      emptyCells = emptyCells.not(botCells[j]);
    }

    outpuNum = Math.round(Math.random() * (emptyCells.size() - 1));
    self = $(emptyCells[outpuNum])[0];
    $(self).text('O');
    $(self).addClass('O');
    $(self).off('click');
  }

  var smarterBot = function () {
    var opponentCells = $('.X').toArray();
    var botCells = $('.O').toArray();
    var emptyCells = $('td');
    for (var i in opponentCells) {
      emptyCells = emptyCells.not(opponentCells[i]);
    }
    for (var j in botCells) {
      emptyCells = emptyCells.not(botCells[j]);
    }
    var found = false;
    emptyCells = emptyCells.toArray();
    for (var k in emptyCells) {
      self = emptyCells[k];
      if (turn > 4) {
        judge('O');
      }
      if (winner) {
        found = true;
        winner = '';
        break;
      }
    }
    if (found !== true ) {
      for (var k in emptyCells) {
        self = emptyCells[k];
        if (turn >= 3) {
          judge('X');
        }
        if (winner) {
          found = true;
          winner = ''
          break;
        }
      }
    }
    if (found !== true ) {
      var outpuNum = Math.round(Math.random() * (emptyCells.length - 1));
      self = $(emptyCells[outpuNum])[0];
    }
    $(self).text('O');
    $(self).addClass('O');
    $(self).off('click');
  }

  var vsBot = function () {
    $('.game_on').fadeOut();
    $('td').on('click', function () {
      self = this;
      $(this).text('X');
      $(this).addClass('X');
      $(this).off('click');
      if (turn >= 4) {
        judge('X');
        announcement('X');
      }
      turn++;

      if (winner === '') {
        smarterBot();
        if (turn > 4) {
          judge('O');
          announcement('O');
        }
        turn++;
      }

    });
  }

  var vsHuman = function () {
    $('.game_on').fadeOut();
    $('td').on('click', function () {
      self = this;
      if (turn%2 === 0) {
        $(this).text('X');
        $(this).addClass('X');
        $(this).off('click');
      }
      else {
        $(this).text('O');
        $(this).addClass('O');
        $(this).off('click');
      }
      if (turn >= 4) {
        judge($(this).attr('class'));
        announcement($(this).attr('class'));
      }
      turn++;
    })
  }

  var gameOver = function () {
    turn = 0;
    $('td').empty();
    $('td').removeClass();
    $('td').css('color','black');
    $('.score').empty();
    winner = '';
    winnerCells = [];
    $('.game_over').fadeOut();
    $('.game_on').fadeIn();
  }
  $('.game_on').fadeIn();
  $('#bot').on('click', vsBot);
  $('#human').on('click', vsHuman);
  $('.reset span').on('click', gameOver);

})