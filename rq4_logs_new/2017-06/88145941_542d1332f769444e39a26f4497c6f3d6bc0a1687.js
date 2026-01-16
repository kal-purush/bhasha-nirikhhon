$(document).ready(function() {

  $('.link').on('click', function(e){

    var hash = $(this).attr('href');
     $('html,body').animate({
       scrollTop: $(hash).offset().top
     }, 800);

    return false;
  });
});