//Main Banner

$('#homebanner').owlCarousel({
    items:1,
    dots:true,
    nav:false,
})



$(window).scroll(function(){
  var sticky = $('header'),
      scroll = $(window).scrollTop();

  if (scroll >= 100){
      sticky.addClass('fixed-hed');
      
  }
  else {sticky.removeClass('fixed-hed');
  }
});

// Faculties Slider & expertFaculties Slider
$(function(){
$(' #facultiesSlider, #expertFaculties, #facultiesSlider1, #facultiesSlider2').owlCarousel({
    items:3,
    margin:10,
    autoplay:true,
    loop:true,
    dots:false,
    nav: false,
    navText: ['<i class="fa-solid fa-chevron-left"></i>','<i class="fa-solid fa-chevron-right"></i>'],
    responsive : {
        0 : {
            items:1.4,
            center:true
        },
        768 : {
           items:3,
           dots:false,
            nav: false
        }
    }
})


})