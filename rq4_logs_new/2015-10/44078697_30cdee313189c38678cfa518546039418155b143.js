$(document).ready(function(){
	$('.menu1').css("background-color","#000066");
	$('.menu2').css("background-color","#000066");
	$('.menu3').css("background-color","#000066");
	$('.menu4').css("background-color","#000066");
	$('.menu1').css("color","white");
	$('.menu2').css("color","white");
	$('.menu3').css("color","white");
	$('.menu4').css("color","white");
	
	$('.menu1').mouseenter(function(){
	
		$('.menu1').css("background-color","white");
		$('.menu1').css("color","black");
		//$(this).animate({width: "95px",height :"95px"},300);
	});
	$('.menu1').mouseleave(function(){
		$('.menu1').css("background-color","#000066");
		$('.menu1').css("color","white");
		//$(this).animate({width: "90px",height : "90px"},30);

	});
	$('.menu2').mouseenter(function(){
	
		$('.menu2').css("background-color","white");
		$('.menu2').css("color","black");
		//$(this).animate({width: "95px",height :"95px"},'slow');

	});
	$('.menu2').mouseleave(function(){
		$('.menu2').css("background-color","#000066");
		$('.menu2').css("color","white");
		//$(this).animate({width: "90px",height : "90px"},'fast');

	});
	$('.menu3').mouseenter(function(){
	
		$('.menu3').css("background-color","white");
		$('.menu3').css("color","black");
		//$(this).animate({width: "95px",height :"95px"},'slow');

	});
	$('.menu3').mouseleave(function(){
		$('.menu3').css("background-color","#000066");
		$('.menu3').css("color","white");
		//$(this).animate({width: "90px",height : "90px"},'fast');

	});
	$('.menu4').mouseenter(function(){
	
		$('.menu4').css("background-color","white");
		$('.menu4').css("color","black");
		//$(this).animate({width: "95px",height :"95px"},'slow');

	});
	$('.menu4').mouseleave(function(){
		$('.menu4').css("background-color","#000066");
		$('.menu4').css("color","white");
	//$(this).animate({width: "90px",height : "90px"},'fast');

	});

	$('.menu-icon').click(function() {
			//if(event.which == 39){
			$('.menu').animate({left: "0px"}, 200);
		    	$('.body').animate({left: "285px"}, 200);
  			//	}
			});
	$('.icon-close').click(function(){
			$('.menu').animate({left:"-285px"},200);
			$('.body').animate({left: "0px"}, 200);
		//	}
	});
	
		

});