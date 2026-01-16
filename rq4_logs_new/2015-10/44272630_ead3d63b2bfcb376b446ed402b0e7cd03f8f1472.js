/************************

@authorName: Ray Masaki
@dateCreated: 10.14.2015
@lastUpdated: 10.14.2015
@projectName: Pickment

************************/

console.log('Loaded');

/*
## 1. User clicks start
## 2. hides intro div
## 3. reveals level count div
## 4. level count div hides after 1 second
5. level count var is set in header and timer begins countdown
6. game shows colorWord from an array with random colorVal from array
7. mouse has corresponding colorVal = colorWord
8. user clicks on colorWord that is equal to colorVal
9. if correct goes to next level
10. if incorrect reveals intro div with updated highscore and start replaced with play again
*/

/* Global Variables */

// May not be color safe look at 16bit colors
var colorVals = [{
  colorName: 'red',
  colorHex: '#F44336'
}, {
  colorName: 'pink',
  colorHex: '#E91E63'
}, {
  colorName: 'purple',
  colorHex: '#9C27B0'
}, {
  colorName: 'indigo',
  colorHex: '#3F51B5'
}, {
  colorName: 'blue',
  colorHex: '#2196F3'
}, {
  colorName: 'cyan',
  colorHex: '#00BCD4'
}, {
  colorName: 'teal',
  colorHex: '#009688'
}, {
  colorName: 'green',
  colorHex: '#4CAF50'
}, {
  colorName: 'lime',
  colorHex: '#CDDC39'
}, {
  colorName: 'yellow',
  colorHex: '#FFEB3B'
}, {
  colorName: 'gold',
  colorHex: '#FFC107'
}, {
  colorName: 'orange',
  colorHex: '#FF9800'
}, {
  colorName: 'brown',
  colorHex: '#795548'
}, {
  colorName: 'grey',
  colorHex: '#9E9E9E'
}];

var pickerColor = 'white';
var $game = $('#game');


/* Game */

$(document).ready(function() {

  var $picker = $('.picker');



  /* Mouse Cursor */
  // http://stackoverflow.com/questions/3385936/jquery-follow-the-cursor-with-a-div

  var mouseMove = function (e) {
      $picker.css({
        left: e.pageX - 35,
        top: e.pageY - 35
      });
  };

  $(document).on('mousemove', mouseMove);

  /* Intro Logic */

  var $startButton = $('.start-button');
  var $intro = $('#intro');

  // Clicking start button will fade out intro and show level count intro
  $startButton.click(function(e) {
    $intro.fadeOut('fast');

    levelCountIntro();
  });



  /* Level Count */

  var levelCount = 17;

  var $levelCountIntro = $('#level-count > h1');
  $levelCountIntro.text("Level " + levelCount);

  // level count intro fades out after 1 second
  var levelCountIntro = function() {

    changePickerColor();

    setTimeout(function() {
      $('#level-count').fadeOut('fast');
    }, 1000);
  };

  /* Picker Color */

  var colorLen = colorVals.length;
  // console.log(colorLen);

  var changePickerColor = function() {
    var randomColor = Math.floor(Math.random() * colorLen);

    var newColorHex = colorVals[randomColor].colorHex;
    var newColorName = colorVals[randomColor].colorName;
    $picker.css('background', newColorHex);

    createCell(levelCount);
  };


  /*
  Logic for creating cells:
  - Math.round(square root of level) = how many rows
  - level/sqrt = how many per row
  - 100 / cells per row = % width of each cell
  - level % sqrt = how many cells on the last row
  - create a colorWord element in the cell
  */

  // 2 factors without modulo

  var createCell = function(levelCount) {
    // for each level count find the sq rt and round to find the amount of rows
    // round down
    var levelRoot = Math.floor(Math.sqrt(levelCount)); // 16 > 4
    var cellPerRow = Math.ceil(levelCount / levelRoot); // 16 / 4 = 4

    for (var i = 0; i < levelRoot; i++) {
      // for each levelRoot make a new row
      var $newRow = $('<div class="row">');

      // for each row make a new cell
      for (var j = 0; j < cellPerRow; j++) {
        var randomColor = Math.floor(Math.random() * colorLen);

        var $newCell = $('<div class="cell">');
        $newCell.text(colorVals[randomColor].colorName);
        // within each cell make a colorWord
        $newRow.append($newCell);
      }

      $game.append($newRow);

    }
  };

/*  var createCell = function(level) {
    for (var i = 0; i < level; i++) {
      var randomColor = Math.floor(Math.random() * colorLen);

      var $newCell = $('<div class="cell">');
      $newCell.text(colorVals[randomColor].colorName);
      // within each cell make a colorWord
      $game.append($newRow);
    }
  };*/


  /*
  colorWord logic:
  - number of colorWord is equal to level number
  - currentColors is an array of colorVals
  - display each colorWord with a noncorresponding colorVal
  - assign mouseColor with one of the colorVals
  - mouseColor clicks colorWord proceed to next level
  */


});