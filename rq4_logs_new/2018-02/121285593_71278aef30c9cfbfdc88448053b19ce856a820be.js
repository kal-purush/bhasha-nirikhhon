$(document).ready(function() {
  $(window).bind('scroll', function() {
    // The value of where the "scoll" is.
    if($(window).scrollTop() > 235){
      $('nav').addClass('fixed');
      $('.container').css('padding-top', '90px');
    }else{
      $('nav').removeClass('fixed');
      // Adding padding so it doesn't jump up
      // when the menu gets fixed.
      $('.container').css('padding-top', '0px');
    }
  })
});