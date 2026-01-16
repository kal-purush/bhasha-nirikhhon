$(document).ready(function() {
	$(".msv-screenshot-overlay").hover(function() {
		$(this).children(".msv-screenshot-border").fadeOut();
	},
	function() {
		$(this).children(".msv-screenshot-border").fadeIn();
	}
	);
	
	
	$(function () {
	  $('[data-toggle="tooltip"]').tooltip()
	})
});


jQuery('ul.nav li.dropdown').hover(function() {
	jQuery(this).find('.dropdown-menu').stop(true, true).show();
	jQuery(this).addClass('open');
}, function() {
jQuery(this).find('.dropdown-menu').stop(true, true).hide();
	jQuery(this).removeClass('open');
	$(".popover").css("display", "none");
});

