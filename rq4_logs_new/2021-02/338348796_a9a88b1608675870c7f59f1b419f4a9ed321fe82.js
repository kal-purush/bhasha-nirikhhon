// $(".panier").mouseenter(
//   function(){
//     $(this).animate({opacity:'1px'},'slow')
//   });







  $(window).on('scroll', function() {
    let scroll = $(window).scrollTop();
    let dh = $(document).height();
    let wh = $(window).width();
    let scrollPercent = (scroll / (dh - wh) ) * 20;
    $('.wave').css({
      'top': `${scrollPercent}%`
    });
  });

 $(".cup").mouseenter(
   function(){
   $('.panierhover').css({opacity:'1'},'slow')
  });

 $(".cup").mouseleave(
   function(){
   $('.panierhover').css({opacity:'0'},'slow')
  });