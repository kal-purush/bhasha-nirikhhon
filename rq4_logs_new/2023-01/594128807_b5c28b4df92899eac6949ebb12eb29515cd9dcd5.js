  var owl = $('.owl-carousel');
  owl.owlCarousel({
    center:true,
    loop:true,
    margin:30 ,
    startPosition:1
});
  // Go to the next item
  $('.slider_btn--next').click(function() {
      owl.trigger('next.owl.carousel');
  })
  $('.slider_btn--prev').click(function() {
      owl.trigger('prev.owl.carousel', [300]);
  })