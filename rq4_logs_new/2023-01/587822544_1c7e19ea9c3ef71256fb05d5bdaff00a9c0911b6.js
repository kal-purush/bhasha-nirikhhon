$('.hero-slider').slick({
  dots: true,
  infinite: true,
  speed: 500,
  fade:true,
   arrows:false,
  draggable:false,
  autoplay:true,
  responsive: [
    {
      breakpoint: 1024,
      settings: {
        slidesToShow: 1,
        slidesToScroll: 1,
        infinite: true,
        dots: true
      }
    },
    {
      breakpoint: 600,
      settings: {
        slidesToShow: 1,
        slidesToScroll: 1
      }
    },
    {
      breakpoint: 480,
      settings: {
        slidesToShow: 1,
        slidesToScroll: 1
      }
    }
  ]
});
$('.services-slider').slick({
 dots: true,
  infinite: false,
  speed: 300,
  slidesToShow: 3,
  arrows:false,
  touchMove: false,
  slidesToScroll: 1,
  responsive: [
    {
      breakpoint: 1024,
      settings: {
        slidesToShow: 3,
        slidesToScroll: 3,
        infinite: true,
        dots: true
      }
    },
    {
      breakpoint: 600,
      settings: {
        slidesToShow: 2,
        slidesToScroll: 2
      }
    },
    {
      breakpoint: 480,
      settings: {
        slidesToShow: 1,
        slidesToScroll: 1
      }
    }
  ]
});

	 $('.testimonial-slider').slick({
 dots: false,
  infinite: true,
  speed: 300,
  
  arrows:true,
  touchMove: false,
});