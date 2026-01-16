$(document).ready({
  $('#Contact').click(function(e){
    $('body').append('<div class=\"popup\"></div>');
    e.preventDefault();
  });
});