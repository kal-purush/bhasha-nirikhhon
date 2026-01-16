$(document).ready(function() {



// sticky header

	function header() {
		var scroll_top = $(window).scrollTop();
		if(scroll_top > 1) {
   			$('.header_wrap').addClass('is-fixed');
   		}
   		else{
   			$('.header').removeClass('is-fixed');
   		}
   		// if ($('.wrapper').css('overflow') == 'hidden') {
   		// 	$('.header-wrap').css('top', scroll_top);
   		// }
   		// else{
   		// 	$('.header-wrap').css('top', 0).removeClass('is-open');
   		// 	$('.menu-btn').removeClass('is-active');
   		// }

	} 
	header();	


	// sliders init
	function slick(){
		$('.js-slick').slick({
			fade: true,
			speed: 600,
			dots: true,
			infinite: true,
			slidesToShow: 1,
			slidesToScroll: 1,
			responsive: [
			    {
			      breakpoint: 400,
			      settings: {
			       dots: false
			      }
			    }
			  ]
		});
		$('.js-slick-2').slick({
			speed: 600,
			infinite: true,
			slidesToShow: 5,
			slidesToScroll: 1,
			responsive: [
				{
			    	breakpoint: 1280,
			    	settings: {
			    		slidesToShow: 4,
			    	}
			    },
			    {
			    	breakpoint: 900,
			    	settings: {
			    		slidesToShow: 3,
			    	}
			    },
			    {
			    	breakpoint: 768,
			    	settings: {
			    		slidesToShow: 2,
			    	}
			    }
			]
		}); 
		// On swipe event
		$('.js-slick').on('afterChange', function(event, slick, direction){
			$('.slick-next').css('opacity', '0');
			$('.slick-prev').css('opacity', '0');

		});

	}
	slick();

// sidebar	
	$('.menu-btn').on('click', function(){
		$(this).addClass('is-active');

		$('.header-wrap').addClass('is-open');
		return false;
	});
	$('.main').on('touchstart click', function(){
		$('.menu-btn').removeClass('is-active');
		$('.header-wrap').removeClass('is-open');

	});

	$('.js-close-sidebar').on('click', function(){
		$('.menu-btn').removeClass('is-active');
		$('.header-wrap').removeClass('is-open');
	});

	$('.out__inner').swipe({
		swipeLeft: function(event, direction, distance, duration, fingerCount) {

		},
		swipeRight: function(event, direction, distance, duration, fingerCount) {
		   	$('.menu-btn').removeClass('is-active');
			$('.header-wrap').removeClass('is-open');
		},
		//Default is 75px, set to 0 for demo so any distance triggers swipe
		threshold: 0
	});

 // nav scroll

    function scrollNav(){
        $('.js-section').each(function(){
            var pos = $(this).offset().top;
            var id = $(this).attr("id");
            if( $(window).scrollTop() >= (pos -112 )){
                $('.menu ul li a').removeClass('is-active');
                $('[href = #section'+id+']').addClass('is-active');
                //if ($(".last .title").text() === "gallery"){
                if(id ==='2'){
                if($(".invisible1").css("display") === "block"){
                	$($('[href = #section'+id+']')[1]).removeClass('is-active');
                	$($('[href = #section'+id+']')[2]).removeClass('is-active');
                	$($('[href = #section'+id+']')[4]).removeClass('is-active');
                	$($('[href = #section'+id+']')[5]).removeClass('is-active');
                }
                //if ($(".last .title").text() === "events"){
                if($(".invisible2").css("display") === "block"){	
                	$($('[href = #section'+id+']')[0]).removeClass('is-active');
                	$($('[href = #section'+id+']')[2]).removeClass('is-active');
                	$($('[href = #section'+id+']')[3]).removeClass('is-active');
                	$($('[href = #section'+id+']')[5]).removeClass('is-active');
                }

                //if ($(".last .title").text() === "news"){
                if($(".invisible3").css("display") === "block"){
                	$($('[href = #section'+id+']')[0]).removeClass('is-active');
                	$($('[href = #section'+id+']')[1]).removeClass('is-active');
                	$($('[href = #section'+id+']')[3]).removeClass('is-active');
                	$($('[href = #section'+id+']')[4]).removeClass('is-active');
                }
            }
            };
        });
    }
    //AJAX
    $(".menu a,.link").click(function (){
        var page = $(this).text().trim().toLowerCase();
        if (page === "gallery"){
    		$('.invisible1').fadeIn( "slow" );//;css({"display":"block"});
    		$('.invisible2').fadeOut( "slow" );//css({"display":"none"});
    		$('.invisible3').fadeOut( "slow" );//css({"display":"none"});
    		gal();
        }
    	if (page === "events"){
    		$('.invisible1').css({"display":"none"});
    		$('.invisible2').css({"display":"block"});
    		$('.invisible3').css({"display":"none"});
    		events();
    	}
    	if (page === "news"){
    		$('.invisible1').css({"display":"none"});
    		$('.invisible2').css({"display":"none"});
    		$('.invisible3').css({"display":"block"});
    		news();
    	}
 
    });


	function	events(){

		$.ajax({
		type:'GET',
		url:'/events',
		success : function(ev) {
		 $('.invisible2 .title').html(ev[0].name);
		 $('.invisible2 .item__square').css({"background":"url("+ev[0].content.image.path+")"});
		 $('.invisible2 .event__title').html(ev[0].content.title);
		 $('.invisible2 .event__disc').html(ev[0].content.text);
		},
		 error: function() {
			alert('error loading orders');
		}
	});

	};

	function	news(){
		
		$.ajax({
			type:'GET',
			url:'/news',
			success : function(n) {
				$('.invisible3 .title').html(n[0].name);
			   for( i in Object.keys(n[0].content)){
			 	$('.invisible3 .list__item:nth-child('+(+i+1)+')').html(n[0].content[i]);
			 }},
			 error: function() {
				alert('error loading orders');
			}
		});
	};

    function gal(){

	$.ajax({
		type:'GET',
		url:'/gallery',
		success : function(gal) {
		 $('.invisible1 .title').html(gal[0].name);
		 //$('.item__square:nth-child('+12+')').css({"background":"url("+gal[0].content[0]+")"});
		   for( i in Object.keys(gal[0].content)){
		 	$('.invisible1 .item__square:nth-child('+(+i+1)+')').css({"background":"url("+gal[0].content[i]+")"});
		 }},
		 error: function() {
			alert('error loading orders');
		}
	});


	};


    $(".link").click(function (){
        var page = $(this).attr("href");
    
        $('html, body').animate({
            scrollTop: $("."+page.slice(1)).offset().top - 110
        }, 500);
        return false;
    });


//resize function

	$(window).resize(function(){
		header();
	});

//scroll function

	$(window).scroll(function(){
		header();
		scrollNav();
	});

	$('.slider button').text("");

});
// var gal = [
//     {   
//         "name": "gallery", 
//         "content": {
//             0: "images/1.jpg",
//             1: "images/2.jpg",
//             2: "images/3.jpg",
//             3: "images/4.jpg",
//             4: "images/5.jpg",
//             5: "images/6.jpg",
//             6: "images/7.jpg",
//             7: "images/8.jpg",
//             8: "images/9.jpg",
//             9: "images/10.jpg",
//             10: "images/11.jpg",
//             11: "images/12.jpg",
//         }
//     }
// ];
// function gallery(gal){
// 	var template1 = $('.last .title').html();
//   	var rendered = Mustache.render(template1,  gal[0]);
//  	$('.last .title').html(rendered);
//     for( i in Object.keys(gal[0].content)){
//  	$('.item__square:nth-child('+i+')').css({"background":"url("+gal[0].content[i]+")"});
//  	}
// };
//gallery();
