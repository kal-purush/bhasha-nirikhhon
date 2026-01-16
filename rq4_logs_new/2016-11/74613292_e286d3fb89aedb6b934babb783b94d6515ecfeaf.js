var jumboHeight = $('.jumbotron').outerHeight();

function parallax() {
  var scrolled = $(window).scrollTop();
  $('.bg').css('height', (jumboHeight - scrolled) + 'px');
}

$(window).scroll(function(e) {
  parallax();
});

$(document).ready(function() {

  $("#owl-demo").owlCarousel({

    autoPlay: 3000, //Set AutoPlay to 3 seconds

    items: 4,
    itemsDesktop: [1199, 3],
    itemsDesktopSmall: [979, 3]

  });

});