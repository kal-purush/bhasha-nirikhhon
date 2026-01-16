$(function() {
	var my_img = $(".my_img");
	var nav_top = $(".top_nav");
	my_img.height(my_img.width());
	nav_top.css("left", $(".left_information").width());
	$(".right_information").css({
		"left": $(".left_information").width(),
		top: $(".top_nav").height()+10,
		width:function(){
			if($(window).width()<1000)
			{return 1000;}
			else
			{return $(window).width()-$(".left_information").width();}
		}
	});
	$(window).on("resize", function() {
		console.log("triggle");
		my_img.height(my_img.width());
		nav_top.css("left", $(".left_information").width());
		$(".right_information").css({
			"left": $(".left_information").width(),
			"top": function() {
				return $(".top_nav").height() + 10;
			},
			width:function(){
			if($(window).width()<1000)
			{return 1000;}
			else
			{return $(window).width()-$(".left_information").width();}
		}
		});
	});
});