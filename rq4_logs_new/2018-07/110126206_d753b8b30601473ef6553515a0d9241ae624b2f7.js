(function(){

$(document).ready(function  (){


	$(window).scroll(function (){
		var scroll = $(window).scrollTop();

			if (scroll > 40 ) {
					$(".nav-holder").addClass("point");
				}else {
					$(".nav-holder").removeClass("point")
				}

		});


	$(window).scroll(function (){
		var scrol = $(window).scrollTop();
		var range = $(".range").offset();
		console.log(range);
		console.log(scrol);

		if (scrol > range.top-100){
			$("nav a").addClass("pasek");
		}else {
				$("nav a").removeClass("pasek");
		}
		


	});




		

	$(".outbox a").on("click", function(e) {
		var target = this.hash,
			$target =$(target);
			// navMain = $(".nav-holder");
			// console.log(navMain);

			
				
			 function getPosition (){
			 		var $pos;

				if (window.innerWidth > 800) {
					$pos = 0;
				} else{
					$pos= 80;
				}
				  return $pos

			};
				
			$('html,body').stop().animate({
				'scrollTop':$target.offset().top - getPosition()

			},900,'swing', function(){

			});	

	});




		$(".button").on("click", function(e) {
			//do buttona
				if ($(".button").hasClass("button-active") && ("button-after")) {
					$(".button").removeClass("button-active"),
					$(".button").removeClass("button-after");
				} else {
					$(".button").addClass("button-active"),
					$(".button").addClass("button-after");
				}

			//do nawigacji
				$(".nav").toggleClass("active");

		});

			$( window ).resize(function() {

				if (window.innerWidth > 768) {
				//do nawigacji
					$(".nav").removeClass("active"),
				//do buttona
					$(".button").removeClass("button-active"),
					$(".button").removeClass("button-after");
				}
			 	
			});



			

		

});


})();