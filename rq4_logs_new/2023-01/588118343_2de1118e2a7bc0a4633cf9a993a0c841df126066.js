// Rating Initialization

$(document).ready(function() {
  $('.bar span').hide();
  $('#bar-five').animate({
     width: '75%'}, 1000);
  $('#bar-four').animate({
     width: '50%'}, 1000);
  $('#bar-three').animate({
     width: '25%'}, 1000);
  $('#bar-two').animate({
     width: '10%'}, 1000);
  $('#bar-one').animate({
     width: '0%'}, 1000);

  setTimeout(function() {
    $('.bar span').fadeIn('slow');
  }, 1000);

});