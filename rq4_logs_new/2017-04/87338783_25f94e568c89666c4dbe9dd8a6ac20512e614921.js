$('.featured-slider').slick({
	dots: false,
	infinite: true,
	speed: 300,
	slidesToShow: 4,
	slidesToScroll: 4,
	autoplay: false,
	autoplaySpeed: 2000,
	responsive: [	
	{
		breakpoint: 769,
		settings: {
			slidesToShow: 3,
			slidesToScroll: 3				
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
    // You can unslick at a given breakpoint now by adding:
    // settings: "unslick"
    // instead of a settings object
    ]
});