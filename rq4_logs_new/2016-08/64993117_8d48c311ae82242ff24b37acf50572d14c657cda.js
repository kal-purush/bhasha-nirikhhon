$(document).ready(function () {
	
	var banner = {
		parent: $('#banner'),
		nSlide: $('#banner').children('.slide').length,
		pos: 1
	}

	var info = {
		parent: $('#info'),
		nSlide: $('#info').children('.slide').length,
		pos: 1
	}

	banner.parent.children('.slide').first().css({
		'left': 0
	});

	info.parent.children('.slide').first().css({
		'left': 0
	});

	var hBanner = function () {
		var h = banner.parent.children('.slide').outerHeight();

		banner.parent.css({
			'height': h + 'px'
		});
	}

	var hInfo = function () {
		var h = info.parent.children('.active').outerHeight();

		info.parent.animate({
			'height': h + 'px'
		});
	}

	var hFlex = function (){
		var h = $()
	}

	hBanner();
	hInfo();

	$(window).resize(function(){
		hBanner();
		hInfo();
	});

	$('#info').children('.slide').each(function() {
		$('#botones').append('<span>');
	});

	$('#botones').children('span').first().addClass('active');



	$('#banner-next').on('click', function(e){
		e.preventDefault();

		if (banner.pos < banner.nSlide){
			banner.parent.children().not('.active').css({
				'left': '100%'
			});

			$('#banner .active').removeClass('active').next().addClass('active').animate({
				'left': 0
			});

			$('#banner .active').prev().animate({
				'left': '-100%'
			});

			banner.pos = banner.pos + 1;

		} else {

			$('#banner .active').animate({
				'left': '-100%'
			});

			banner.parent.children().not('active').css({
				'left': '100%'
			});

			$('#banner .actve').removeClass('active');
			banner.parent.children().first().addClass('active').animate({
				'left': 0
			});

			banner.pos = 1;

		}

	});


	$('#banner-prev').on('click', function(e){
		e.preventDefault();

		if (banner.pos > 1) {

			banner.parent.children().not('.active').css({
				'left': '-100%'
			});

			$('#banner .active').animate({
				'left': '100%'
			});

			$('#banner .active').removeClass('active').prev().addClass('active').animate({
				'left': 0
			});

			banner.pos = banner.pos -1;

		} else {

			banner.parent.children().not('.active').css({
				'left': '-100%'
			});

			$('#banner .active').animate({
				'left': '100%'
			});

			$('#banner .active').removeClass('active');
			banner.parent.children().last().addClass('active').animate({
				'left': 0
			});

			banner.pos = banner.nSlide;
		}

	});

$('#info-next').on('click', function(e){
		e.preventDefault();

		if (info.pos < info.nSlide){
			info.parent.children().not('.active').css({
				'left': '100%'
			});

			$('#info .active').removeClass('active').next().addClass('active').animate({
				'left': 0
			});

			$('#info .active').prev().animate({
				'left': '-100%'
			});

			$('#botones').children('.active').removeClass('active').next().addClass('active');

			info.pos = info.pos + 1;

		} else {

			$('#info .active').animate({
				'left': '-100%'
			});

			info.parent.children().not('active').css({
				'left': '100%'
			});

			$('#info .actve').removeClass('active');
			info.parent.children().first().addClass('active').animate({
				'left': 0
			});

			$('#botones').children('.active').removeClass('active');
			$('#botones').children('span').first().addClass('active');

			info.pos = 1;

		}

	});


	$('#info-prev').on('click', function(e){
		e.preventDefault();

		if (info.pos > 1) {

			info.parent.children().not('.active').css({
				'left': '-100%'
			});

			$('#info .active').animate({
				'left': '100%'
			});

			$('#info .active').removeClass('active').prev().addClass('active').animate({
				'left': 0
			});

			$('#botones').children('.active').removeClass('active').prev().addClass('active');

			info.pos = info.pos -1;

		} else {

			info.parent.children().not('.active').css({
				'left': '-100%'
			});

			$('#info .active').animate({
				'left': '100%'
			});

			$('#info .active').removeClass('active');
			info.parent.children().last().addClass('active').animate({
				'left': 0
			});

			$('#botones').children('.active').removeClass('active');
			$('#botones').children('span').last().addClass('active');

			info.pos = info.nSlide;
		}

	});

});