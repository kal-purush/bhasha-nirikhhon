/* To hide and show the titles of the boxes */
$(document).ready(function() {
	$('.box header span label').hide();
	$('.box').hover(function() {
		$(this).children('header').children('span').children('i').hide();
		$(this).children('header').children('span').children('label').show();

	}, function() {
		$(this).children('header').children('span').children('i').show();
		$(this).children('header').children('span').children('label').hide();
	});
});