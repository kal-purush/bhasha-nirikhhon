$(window).on("scroll", function(){
	$("nav").toggleClass("scrolled", parseInt($(window).scrollTop(), 10) > 100);
})

$("a[href^='#']").on("click", function(e){
	e.preventDefault();

	var section = $(this.hash);
	var top = section.offset().top;

	$("html, body").animate({ scrollTop: top - 450 })
});