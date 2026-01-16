// Check off tasks when clicked
$("ul").on("click", "li", function(){
	$(this).toggleClass("complete");
});

// Click on x to delete task
$("ul").on("click", "li span", function(e){
	$(this).parent().fadeOut(500, function(){
		$(this).remove();
	});
	e.stopPropagation();
});

$("input[type='text']").on("keypress", function(e){
	if(e.which === 13){
		var td_text = $(this).val();
		$(this).val("");
		$("ul").append("<li><span><i class='fa fa-trash'></i></span> " + td_text + "</li>");
	}
});

$(".fa-plus").on("click", function(){
	$("input").fadeToggle(400, function(){
		$(this).toggleClass("hidden");
	});
});