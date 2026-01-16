$(document).ready(function() {

	$(window).load(function() { 
		$(".loader_inner").fadeOut(); 
		$(".loader").delay(400).fadeOut("slow"); 
	});

	// Owl_Carousel //
	$('.owl-carousel').owlCarousel({
	animateOut: 'slideOutLeft',
    animateIn: 'fadeIn',
	autoplay:true,
	autoplaySpeed:2500,
	navigation : true,
	autoplayHoverPause : true,
    loop:true,
    margin:10,
    responsiveClass:true,
    responsive:{
        0:{
            items:1,
            nav:true
        },
        600:{
            items:1,
            nav:true,
        },
        1000:{
            items:1,
            nav:true,
            loop:true,
            navSpeed: 1500,
            dots: true,
        }
    }
})



	//Animated section "Skills "
	$(".s_resume").waypoint(function(){
		$(".star_red").each(function(index) {
			var ths = $(this);
			var atr = ths.data("width");
			var res = atr + "%";
			setTimeout(function() {
				ths.animate({width: res});
			}, 100*index);
		});
		

	}, {
		offset: "25%"
	});


	//Animated
	$(".top_text h1").animated("slideInDown","fadeOutUp");
	$(".about_foto img").animated("flipInY","fadeOutUp");
	$(".about_text p").animated("fadeInLeft","fadeOutUp");
	$(".str_name, .about_info ul li").animated("fadeInRight","fadeOutUp");
	$(".skill_txt").animated("fadeInLeft","fadeOutUp");
	$(".lable_star").animated("fadeInRight","fadeOutUp");
	$(".owl-stage-outer").animated("fadeIn","bounceOut");
	$(".section_form").animated("fadeInRight","fadeOutUp");
	$(".sector_contact").animated("fadeInUp","fadeOutUp");



	//Page2ID
	$(".top_menu ul li a").mPageScroll2id();
	$(".mousey a").mPageScroll2id();
	


 	//MENU_toggle
	
	$(".toggle_mnu").click(function() {
		$(".toggle-mnu").toggleClass("active");
	});

	$(".top_menu ul a").click(function(){
		$(".top_menu").fadeOut(600);
		$(".toggle-mnu").toggleClass("active");
		$(".top_text").css("opacity", "1");
	});
	
	$(".toggle_mnu").click(function() {
		if ($(".top_menu").is(":visible")) {
			$(".top_menu").fadeOut(600);
			$(".top_text").css("opacity", "1");
			$(".top_menu li a").removeClass("fadeInUp animated");
			
	} else {
			$(".top_menu").fadeIn(600);
			$(".top_text").css("opacity", "0.1");
			$(".logo_2").css("z-index","255");
			$(".top_menu li a").addClass("fadeInUp animated");
		}
		
	});
	$(".top_menu li a").click(function() {
		$(".top_menu").fadeOut(600);

	});



	//respons heigh
	function heightDetect() {
	$(".main_head").css("height", $(window).height());
	};
	heightDetect();
	$(window).resize(function(){
		heightDetect();
	});

	//E-mail Ajax Send
	$(".s_form").submit(function() { //Change
		var th = $(this);
		$.ajax({
			type: "POST",
			url: "mail.php", //Change
			data: th.serialize()
		}).done(function() {
			alert("Спасибо ваше сообщение отправлено!");
			setTimeout(function() {
				// Done Functions
				th.trigger("reset");
			}, 1000);
		});
		return false;
	});

});

