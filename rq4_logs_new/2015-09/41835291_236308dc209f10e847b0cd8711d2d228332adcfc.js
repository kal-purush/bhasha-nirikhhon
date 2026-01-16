$(document).ready(function(){
  var menu = $("#menu_collapse");
  var nav = $("nav ul");

  menu.on('click', function(e){
    e.preventDefault();
    nav.slideToggle();
    
  }

  );

  $(window).resize(function(){
    var w = $(window).width();
    if(w > 320 && nav.is(':hidden')) {
        nav.removeAttr('style');
    }
});

});