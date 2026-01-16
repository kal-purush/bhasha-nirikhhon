
$(document).ready(function(){
    $('.slides').slick({
        autoplay: true,
        autoplaySpeed:4000
    });
    // MAGNIFIC POPUP
    $('.gallery-image-link').magnificPopup({
		type: 'image',
		tLoading: 'Loading image #%curr%...',
		mainClass: 'mfp-img-mobile',
		gallery: {
			enabled: true,
			navigateByImgClick: true,
			preload: [0,1] // Will preload 0 - before current, and 1 after the current image
		},
		image: {
			tError: '<a href="%url%">The image #%curr%</a> could not be loaded.',
			titleSrc: function(item) {
				return item.el.attr('title') + '<small></small>';
			}
		}
	});
//Smooth scroll
$('.menu-links a').smoothScroll({
offset: -90,
speed:1500,
afterScroll: function(){
	$(this).closest('.menu-links ').find('a').removeClass('active');
	$(this).addClass('active');
}

});

  });
      