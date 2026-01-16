$(function(){
  setInterval(function(){
     $(".slideshow ul").animate({marginLeft:-500},800,function(){
        $(this).css({marginLeft:0}).find("li:last").after($(this).find("li:first"));
     })
  }, 3500);
});