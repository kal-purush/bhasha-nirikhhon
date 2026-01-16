// Select color input
// Select size input


// When size is submitted by the user, call makeGrid()
$(document).ready(function () {
  $("#sizePicker").submit(function (event) {
    event.preventDefault();

    var row;
    var column;
    var color;

    row = $("#inputHeight").val();   // height
    column = $("#inputWidth").val();   // width

    makeGrid(row, column);
     addColor();

  });
});

function makeGrid(height, width) {
  $("tr").remove();

  // Your code goes here!
  for (var x = 1; x <= height; x++) {
    $("#pixelCanvas").append("<tr></tr>");

    for (var y = 1; y <= width; y++) {
      $('tr').filter(':last').append('<td></td>');
    }
  }
}

//add color to clicked cell
function addColor() {
    var cells = $('td');

    cells.click(function () {
       color = $('#colorPicker').val(); //extract the current value of the color picker

      //check for style attribute and remove or add it.
      if ($(this).attr('style')) {
        $(this).removeAttr('style');
      } else {
        $(this).attr('style', 'background-color:' + color);
      }
    });
  }