
//substitutes hello world for your name
  // $(function() {
  //   var yourName = prompt('What is your name?');
  //   $('#name').html(", " + yourName + "!");
  //   });


//on click, run the "add to list" function
$(function() {
  var $wordList = $('#word-list');
  // var $button = $('#new-thing-button');
  $('#new-word-button').on('click', function(event) {
    event.preventDefault();
    MyApp.addToList($wordList);
  });
});

var MyApp = {};

// adds things to the list
MyApp.addToList = function(list) {
  var $newLi = $('<li class="word">');
  var $newItemText = $('#new-word');
  $newLi.html($newItemText.val());
  $newItemText.val('');
  if ($newLi.html() !== '') {
    list.append($newLi);
  }
};


$('#pig-latin-button').on('click', function(event) {
  event.preventDefault();

  var word = $( ".word" );

  var word_string = word.each(function( index ) {
    console.log($( this ).text() );
  });

  
});