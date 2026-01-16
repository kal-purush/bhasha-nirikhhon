$("#contactBtn").on('click',function(){
	$("#orderBtn").css({
		"background-color":"#fff"
	});
	$("#contactBtn").css({
		"background-color":"rgba(224, 234, 220,.8)"
	});
	$('#content2').fadeOut(100,function(){
		$('#content1').fadeIn(1000);
	});
	
});
$("#orderBtn").on('click',function(){
	$("#contactBtn").css({
		"background-color":"#fff"
	});
	$("#orderBtn").css({
		"background-color":"rgba(224, 234, 220,.8)"
	});
	$('#content1').fadeOut(100,function(){
		$('#content2').fadeIn(1000);
	});
});

// ハンバーガーメニューの表示・非表示
$(".openbtn1").on('click',function(){
	var btn = $(".openbtn1").hasClass('active');
	if(btn == false) {
		$(this).toggleClass('active');
		$(".nav-items-sp").css("display","block");
	}else {
		$(this).removeClass('active');
		$(".nav-items-sp").css("display","none");
	}
});

$(".nav-items-sp").on('click',function(){
	$(".nav-items-sp").css("display","none");
	$(".openbtn1").removeClass('active');
});