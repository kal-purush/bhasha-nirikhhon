$(document).ready( function() {
  $("#lightSlider").lightSlider({
          item:1,
      });
  arrow_size();
  $( window ).on('resize', function(event) {
    event.preventDefault();
    arrow_size();
  });
  process__image_animation()

  $(window).scroll(function (event) {
    var scroll = $(window).scrollTop();
    var location_div =  $('.location__container').position().top  - $('.navbar-header').height()
    if (location_div >= scroll) {
      $('.location__image').addClass('animated zoomIn')
    }

  });
});

$(".navbar-nav li a").click(function (event) {
  var toggle = $(".navbar-toggle").is(":visible");
  if (toggle) {
    $(".navbar-collapse").collapse('hide');
  }
});

function arrow_size () {
  $diagonal_arrows = $( ".process__cols:nth-child(2n+1) .process__arrow" );
  if (window.matchMedia("(min-width: 450px) and (max-width: 768px)").matches) {
    $diagonal_arrows
      .removeClass('fa-arrow-right')
      .addClass('fa-long-arrow-right')
      .css({
        'font-weight': 'bold',
        'font-size': '22px'
      });
  }else {
    $diagonal_arrows
      .removeClass('fa-long-arrow-right')
      .addClass('fa-arrow-right')
      .css({
        'font-weight': 'normal',
        'font-size': '15px'
      });
    if (window.matchMedia("(min-width: 768px)").matches){
     $diagonal_arrows.css('font-size', '18px');
    }

  }
}

function process__image_animation() {
  if (window.matchMedia("(min-width: 768px)").matches) {
    var i = 1;
    var interval__animation = setInterval(function () {
      $('.process__cols:nth-child('+ i +') ').removeClass('hidden')
      $('.process__cols:nth-child('+ i +') .process__images-circle')
        .addClass('animated fadeInDownBig');
      i++
      if( i == 10 ){
        clearInterval(interval__animation)
      }
    }, 400)

  } else {
    $('.process__cols').removeClass('hidden')
  }
}

$("#home").on('click', function(event) {
  event.preventDefault();
  $("#section1").animatescroll({scrollSpeed:2000,easing:'easeInOutBack'});
});
$("#process").on('click', function(event) {
  event.preventDefault();
  $("#section2").animatescroll({scrollSpeed:2000,easing:'easeInOutBack'});
});
$("#location").on('click', function(event) {
  event.preventDefault();
  $("#section3").animatescroll({scrollSpeed:2000,easing:'easeInOutBack'});
});
$("#history").on('click', function(event) {
  event.preventDefault();
  $("#section4").animatescroll({scrollSpeed:2000,easing:'easeInOutBack'});
});
$("#gallery").on('click', function(event) {
  event.preventDefault();
  $("#section5").animatescroll({scrollSpeed:2000,easing:'easeInOutBack'});
});






