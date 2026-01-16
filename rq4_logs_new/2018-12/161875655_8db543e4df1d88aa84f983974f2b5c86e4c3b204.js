var $$transform;
var isHandheld = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
var mobilePlatform = isMobile();
var mobileCheck = mobilePlatform && $(window).width() < 960;
var windowWidth;
var windowHeight;
var scrollThreshold_1_2;
var scrollThreshold_1_3;
var scrollThreshold_1_5;
var scrollThreshold_2;
var scrollType;
var playing = false;

var is_form = false;
var is_gallery = false;

var $headerPic;
var $headerPass;
var $menuShowPass;
var $menuBack = false;
var mainMenuOpen = false;

var arrivalDatepicker;
var departureDatepicker;
var hideDatepickerArrival;
var hideDatepickerDeparture;
var arrivalDate = new Date()
var departureDate = new Date();

$(window).load(function () {

	if (scrollType == "iScroll") {
		myScroll.refresh();
	}

	window[current + '_load']();

	// SECOND CHECK //

	if (current != "index") {
		common_offsets();
	}

	if (action == "index" || action == "macro") {
		set_offset();
	}

	if (action == "detail") {
		set_offset_detail();
	}


});

$(window).bind("pageshow", function (event) {
	if (event.originalEvent.persisted) {
		window.location.reload()
	}
});

$(document).ready(function () {

	$('.alerter').bind('click', function (e) {
		e.preventDefault();
		alert('We are currently renewing our Special Promotions, they will be available soon!');
	});

	windowWidth = $(window).width();
	windowHeight = $(window).height();

	$headerPass = windowHeight + 240;
	$menuShowPass = windowHeight;

	if (window.innerWidth > window.innerHeight) {
		$viewport = 'landscape';
	} else {
		$viewport = 'portrait';
	}

	pageY = 0;

	$altLayout = $('#top').hasClass('alt_type');

	scrollThreshold_1_2 = windowHeight / 1.2;
	scrollThreshold_1_3 = windowHeight / 1.3;
	scrollThreshold_1_5 = windowHeight / 1.5;
	scrollThreshold_2 = windowHeight / 2;

	if (!isHandheld) {
		$parallax = true;
	} else {
		$('#widget_meteo').css('border', 'none');

		$parallax = false;
		$('#top').height(windowHeight);
		$('#top.alt_type').height(windowHeight - 140);


		if (isHandheld && windowWidth < 960) { // se Ã¨ tablet portrait
			if (current != "index") {
				$('#top').height(windowHeight / 2 + 80);
				$('#top.alt_type').height(windowHeight / 2);
				$('.fullscreen_pic').height(windowHeight / 2);

				$('#top.alt_type #section_logo .top_img').height(windowHeight / 2);
			}

			scrollThreshold_1_2 = windowHeight / 1.1;
			scrollThreshold_1_3 = windowHeight / 1.1;
			scrollThreshold_1_5 = windowHeight / 1.1;
			scrollThreshold_2 = windowHeight / 1.1;

			if ($viewport == "landscape") {
				$('#top').height(windowHeight);
				$('#top.alt_type').height(windowHeight - 80);
				$('.fullscreen_pic').height(windowHeight);
				$('#top.alt_type #section_logo .top_img').height(windowHeight - 80);
			}

		} else {
			$('#top.alt_type #section_logo .top_img').height(windowHeight);
			$('.fullscreen_pic').height(windowHeight);


		}


		$('#home_logo .logo_pieces .logo_p._1').attr('src', 'http://alexanderlaut.com/images/nlogo/logo2.png');
		$('#home_logo .logo_pieces .logo_p._2').attr('src', 'http://alexanderlaut.com/images/nlogo/logo2.png');
		$('#home_logo .logo_pieces .logo_p._3').attr('src', 'http://alexanderlaut.com/images/nlogo/logo3.png');
		$('#home_logo .logo_pieces .logo_p._4').attr('src', 'http://alexanderlaut.com/images/nlogo/logo4.png');
		$('#home_logo .logo_pieces .logo_p._5').attr('src', 'http://alexanderlaut.com/images/nlogo/logo5.png');

		$('.panel_logo .logo_pieces .logo_p._1').attr('src', 'http://alexanderlaut.com/images/nlogo/star.png');
		$('.panel_logo .logo_pieces .logo_p._2').attr('src', 'http://alexanderlaut.com/images/nlogo/star.png');
		$('.panel_logo .logo_pieces .logo_p._3').attr('src', 'http://alexanderlaut.com/images/nlogo/star.png');
		$('.panel_logo .logo_pieces .logo_p._4').attr('src', 'http://alexanderlaut.com/images/nlogo/star.png');
		$('.panel_logo .logo_pieces .logo_p._5').attr('src', 'http://alexanderlaut.com/images/nlogo/star.png');

		$('#photogallery.detail #top, #photogallery.detail .fullscreen_pic').height(windowHeight);

	}


	if (!Modernizr.touchevents) {
		scrollType = "iScroll";
		myScroll = new IScroll('#main', {
			mouseWheel: true,
			scrollbars: true,
			probeType: 2,
			disablePointer: true,
			scrollY: true,
			scrollX: false,
			useTransition: false,
			interactiveScrollbars: true,


		});


		myScroll.on('scroll', function () {
			pageY = -(myScroll.y);
			common_scroll();
			if (current != "index") {
				block_scroll();
			}
			window[current + '_scroll']();


		});

	} else {
		scrollType = "native";

		$('body').addClass('native-scroll');

		$('#main').css({
			'overflow': 'visible',
			'will-change': 'transform',
			'height': 'auto'
		});

		$('#main_scroller').css({
			'will-change': 'transform',
			'overflow': 'hidden'
		});


		$(window).scroll(function () {
			pageY = window.pageYOffset;
			common_scroll();
			if (current != "index") {
				block_scroll();
			}
			window[current + '_scroll']();



		})
	}


	$$transform = [Modernizr.prefixed('transform')];


	$headerPic = $('.top_img img')[0];

	window[current + '_ready']();




	// FIRST CHECK //

	if (current != "index") {
		common_offsets();
	}

	if (action == "index" || action == "macro") {
		set_offset();
	}

	if (action == "detail") {
		set_offset_detail();
	}


	arrivalDatepicker = $('.arrival').glDatePicker({
		cssName: 'flatwhite',
		zIndex: 1,
		borderSize: 0,
		selectableDateRange: [{
			from: new Date(),
			to: new Date(3000, 1, 1)
		}],
		monthNames: getMonthNames(),
		dowNames: getDowNames(),
		onShow: function (calendar) {
			arrivalDatepicker.render();
			clearTimeout(hideDatepickerArrival);
			calendar.find('div').addClass('top_double').addClass('has_transition_600');
			calendar.show();
			calendar.find('div').each(function (c) {
				setTimeout(function () {
					calendar.find('div:eq(' + c + ')').removeClass('top_double')
				}, 5 * c);
			})

		},
		onHide: function (calendar) {
			hideDatepickerArrival = setTimeout(function () {
				calendar.addClass('has_transition_300').addClass('no_opacity')
				setTimeout(function () {
					calendar.hide();
				}, 300)
			}, 1);
		},
		onClick: function (el, cell, date, data) {
			var dd = date.getDate();
			var mm = date.getMonth() + 1;
			var yyyy = date.getFullYear();

			if (dd < 10) {
				dd = '0' + dd
			}

			if (mm < 10) {
				mm = '0' + mm
			}


			selected = dd + '/' + mm + '/' + yyyy;
			el.children('input').val(selected);
		},

	}).glDatePicker(true);
	departureDatepicker = $('.departure').glDatePicker({
		cssName: 'flatwhite',
		zIndex: 1000,
		borderSize: 0,
		selectableDateRange: [{
			from: new Date(),
			to: new Date(3000, 1, 1)
		}],
		monthNames: getMonthNames(),
		dowNames: getDowNames(),
		onShow: function (calendar) {
			clearTimeout(hideDatepickerDeparture);
			departureDatepicker.render();
			calendar.find('div').addClass('top_double').addClass('has_transition_600')
			calendar.show();
			calendar.find('div').each(function (c) {
				setTimeout(function () {
					calendar.find('div:eq(' + c + ')').removeClass('top_double')
				}, 5 * c);
			})

		},
		onHide: function (calendar) {
			hideDatepickerDeparture = setTimeout(function () {
				calendar.addClass('has_transition_300').addClass('no_opacity')
				setTimeout(function () {
					calendar.hide();
				}, 300)
			}, 1);

		},
		onClick: function (el, cell, date, data) {
			var dd = date.getDate();
			var mm = date.getMonth() + 1;
			var yyyy = date.getFullYear();

			if (dd < 10) {
				dd = '0' + dd
			}

			if (mm < 10) {
				mm = '0' + mm
			}


			selected = dd + '/' + mm + '/' + yyyy;
			el.children('input').val(selected);
		},
	}).glDatePicker(true);


	if (isHandheld) {
		setTimeout(function () {
			$(window).trigger('scroll')
		}, 500);
	}

	$('.menu_controller').bind('click', openMenu);
	// $('.book_button').bind('click',openBookPanel);
	$('.menu_section .macro').bind('mouseenter', showSubmenu);
	$('#menu_container').bind('mouseleave', hideSubmenu);
	$('#main_menu .panel_close').bind('click', close_menu);
	$('#book_panel .panel_close').bind('click', closeBookPanel);
	$('.lang_button').bind('click', set_lang);



	$('.submenu_item').bind('mouseenter', hoverSubmenuItem);
	$('.submenu_item').bind('mouseleave', outSubmenuItem);

	$('.url_manager').bind('click', manage_links);

	$('.send').bind('click', mailer);

	$('.top_img img').imagesLoaded(function () {
		autosize($('.top_img img'));

		$('#main_veil').addClass('no_width');
		$('.top_img_container').removeClass('scaled_up');
		$('.top_img img').removeClass('from_left');

		run_clocks();

		if (current != "index") {
			common_offsets();
		}

		setTimeout(function () {
			$('#top .top_veil').removeClass('no_opacity');
		}, 500);



		if (current == "index") {
			setTimeout(function () {
				$('#home_logo .logo_pieces .logo_p._1').removeClass('top_single');
			}, 1250)
			setTimeout(function () {
				$('#home_logo .logo_pieces .logo_p._2').removeClass('top_single');
			}, 1350)
			setTimeout(function () {
				$('#home_logo .logo_pieces .logo_p._3').removeClass('top_single');
			}, 1450);


			setTimeout(function () {
				$('#home_logo .logo_square .border').removeClass('no_width').removeClass('no_height');

			}, 1800)

			setTimeout(function () {
				$('#home_logo .logo_pieces .logo_p._4').removeClass('top_single');

			}, 1700)
			setTimeout(function () {
				$('#home_logo .logo_pieces .logo_p._5').removeClass('top_single');

			}, 1800);

			$('#home_logo .star').each(function (s) {
				setTimeout(function () {
					$('#home_logo .star:eq(' + s + ')').removeClass('hidden_by_scaling_full');
				}, 1900 + (100 * s));
			});

			setTimeout(function () {
				//$('.pay_top').removeClass('top_hidden')
			}, 2000);


			setTimeout(function () {
				$('.pay_line .square').removeClass('hidden_by_scaling_full');
			}, 1500);

			setTimeout(function () {
				$('.pay_line .square .inline').removeClass('hidden_by_scaling_full');
			}, 1750);

			setTimeout(function () {
				$('.pay_line .line').removeClass('hidden');
			}, 1850);


			setTimeout(function () {
				$('.pay_top').removeClass('top_hidden')
				$('.pay_sub').removeClass('bottom_hidden')
			}, 2200);
		} else {
			setTimeout(function () {
				$('#vm_borders > div').removeClass('no_width').removeClass('no_height');
			}, 500)

			setTimeout(function () {
				$('#vm_borders .small_vm').removeClass('no_width').removeClass('top_single');
			}, 900)




			if ($('#section_title').hasClass('with_borders')) {

				if (current != "lifestyle" || (current == "lifestyle" && action != "detail")) {

					setTimeout(function () {

						$('#section_title .title_borders > div').removeClass('no_width').removeClass('no_height');

					}, 1300);

					setTimeout(function () {
						$('#section_title').removeClass('hidden');
					}, 1400);

					setTimeout(function () {
						$('.title_macro').removeClass('hidden_from_left');

						$('.title_section').removeClass('hidden_from_right');
					}, 1800);

				} else {
					setTimeout(function () {

						$('#section_title .title_borders > div').removeClass('no_width').removeClass('no_height');

					}, 1800);

					setTimeout(function () {
						$('#section_title').removeClass('hidden');
					}, 1900);

					setTimeout(function () {
						$('.title_macro').removeClass('hidden_from_left');

						$('.title_section').removeClass('hidden_from_right');
					}, 2300);
				}

			} else {

				setTimeout(function () {
					$('#section_title').removeClass('hidden');
				}, 1400)

				setTimeout(function () {
					$('#section_title .section_title_typo').removeClass('top_hidden');
				}, 1800);
			}

		}


		setTimeout(function () {
			$('.menu_controller .white_button_separator').removeClass('no_width');
		}, 1000)

		setTimeout(function () {
			$('.menu_controller p').removeClass('top_hidden');
		}, 1200)

		setTimeout(function () {
			$('.book_now_bar .vertical').removeClass('no_height');
		}, 1000)

		setTimeout(function () {
			$('.book_now_bar .white_button_separator').removeClass('no_width');
		}, 1000)

		setTimeout(function () {
			$('.book_now_bar p._1').removeClass('top_hidden');
			$('.book_now_bar p._2').removeClass('bottom_hidden');

		}, 1200);

		setTimeout(function () {
			$('#widget_watch').removeClass('hidden_by_scaling_low');
		}, 2100)

		setTimeout(function () {
			$('#clock').removeClass('no_opacity');
		}, 2200)

		setTimeout(function () {
			$('.location_label').removeClass('no_opacity');
		}, 2300)

		setTimeout(function () {
			$('#widget_date').removeClass('top_single');
		}, 2500)

		setTimeout(function () {
			$('#widget_meteo').removeClass('top_single');
		}, 2600);
		setTimeout(function () {
			$('#audio_player').removeClass('top_single');
		}, 2700)

		setTimeout(function () {
			$('#scroll_down .line').addClass('scroll_loop');
		}, 2000);

		setTimeout(function () {
			$('#scroll_down .start_square').removeClass('hidden_by_scaling_full');
		}, 2500);

		setTimeout(function () {
			$('#scroll_down .square.shifted').removeClass('hidden_by_scaling_full');
		}, 2500);

		setTimeout(function () {
			$('#scroll_down .square.shifted .inline').removeClass('hidden_by_scaling_full');
		}, 2700);
		setTimeout(function () {
			$('#scroll_down .square').removeClass('hidden_by_scaling_full');
		}, 2750);

		setTimeout(function () {
			$('#scroll_down .square .inline').removeClass('hidden_by_scaling_full');
		}, 2950);

	})

	$('.fullscreen_pic img').imagesLoaded(function () {

		if ($('#gallery_container').length != 0) {
			gallery_ready();
			is_gallery = true;

			$('.pic_big').addClass('no_transition');
			$('.pic_big.active').css('z-index', 1).siblings().css('z-index', 2);
			$(".pic_big.active").nextAll().css("transform", "translate3d(" + containerWidth + "px, 0, 0)");
			$(".pic_big.active").prevAll().css("transform", "translate3d(" + (-containerWidth) + "px, 0, 0)");


		}


		// THIRD  CHECK //  
		if (current != "index") {
			common_offsets();
		}

		if (action == "index" || action == "macro") {
			set_offset();
		}

		if (action == "detail") {
			set_offset_detail();
		}
	});

});


$(window).resize(function () {
	windowWidth = $(window).width();
	windowHeight = $(window).height();



	isHandheld = /Android|webOS|iPhone|iPad|iPod|BlackBerry|IEMobile|Opera Mini/i.test(navigator.userAgent);
	mobileCheck = mobilePlatform;

	if (isHandheld) {
		manage_top_resize();
	}

	scrollThreshold_1_2 = windowHeight / 1.2;
	scrollThreshold_1_3 = windowHeight / 1.3;
	scrollThreshold_1_5 = windowHeight / 1.5;
	scrollThreshold_2 = windowHeight / 2;


	if (isHandheld && windowWidth < 960) { // se Ã¨ tablet portrait

		scrollThreshold_1_2 = windowHeight / 1.1;
		scrollThreshold_1_3 = windowHeight / 1.1;
		scrollThreshold_1_5 = windowHeight / 1.1;
		scrollThreshold_2 = windowHeight / 1.1;

	}


	$headerPass = windowHeight + 100;

	if (current != "index") {
		common_offsets();
	}

	if (action == "index" || action == "macro") {
		set_offset();
	}

	if (action == "detail") {
		set_offset_detail();
	}

	if (scrollType == "iScroll") {
		setTimeout(function () {
			myScroll.refresh()
		}, 500);
	}

	if (scrollType == "native") {
		setTimeout(function () {
			$(window).trigger('scroll');
		}, 100)

	}

	autosize($('.top_img img'));

	if (is_gallery) {
		gallery_resize();
	}




})



function manage_top_resize() {
	if (window.innerWidth > window.innerHeight && $viewport == 'portrait') {
		$('#top').height(windowHeight);
		$('#top.alt_type').height(windowHeight - 140);
		$('.fullscreen_pic').height(windowHeight / 2);

		if (isHandheld && windowWidth < 960) { // se Ã¨ tablet portrait

			$('#top').height(windowHeight);
			$('#top.alt_type').height(windowHeight - 80);
			$('.fullscreen_pic').height(windowHeight);
			$('#top.alt_type #section_logo .top_img').height(windowHeight - 80);


			scrollThreshold_1_2 = windowHeight / 1.1;
			scrollThreshold_1_3 = windowHeight / 1.1;
			scrollThreshold_1_5 = windowHeight / 1.1;
			scrollThreshold_2 = windowHeight / 1.1;

		} else {
			$('#top.alt_type #section_logo .top_img').height(windowHeight);
			$('.fullscreen_pic').height(windowHeight);


		}

		$('#photogallery.detail #top, #photogallery.detail .fullscreen_pic').height(windowHeight);
		autosize($('.top_img img'));
		$viewport = 'landscape'


	} else if (window.innerWidth < window.innerHeight && $viewport == 'landscape') {
		$('#top').height(windowHeight);
		$('#top.alt_type').height(windowHeight - 140);
		$('.fullscreen_pic').height(windowHeight / 2);

		if (isHandheld && windowWidth < 960) { // se Ã¨ tablet portrait
			if (current != "index") {
				$('#top').height(windowHeight / 2 + 80);
				$('#top.alt_type').height(windowHeight / 2);
				$('.fullscreen_pic').height(windowHeight / 2);
				$('#top.alt_type #section_logo .top_img').height(windowHeight / 2);
			}

			scrollThreshold_1_2 = windowHeight / 1.1;
			scrollThreshold_1_3 = windowHeight / 1.1;
			scrollThreshold_1_5 = windowHeight / 1.1;
			scrollThreshold_2 = windowHeight / 1.1;

		} else {
			$('#top.alt_type #section_logo .top_img').height(windowHeight);
			$('.fullscreen_pic').height(windowHeight);


		}

		$('#photogallery.detail #top, #photogallery.detail .fullscreen_pic').height(windowHeight);
		autosize($('.top_img img'));
		$viewport = 'portrait';


	}


	;
}


function set_lang(e) {
	e.preventDefault();
	current_url = window.location.href;
	if (current_url.indexOf("/" + lang + "/") == -1) {
		window.location = "/" + $(this).attr("rel") + "/home"
	} else {
		new_url = current_url.replace("/" + lang + "/", "/" + $(this).attr("rel") + "/");
		window.location = new_url
	}
}

function manage_links(e) {
	e.preventDefault();
	url = $(this).attr('href');

	if (mainMenuOpen) {
		setTimeout(clearMenu, 1);
		$('.macro').unbind().addClass('pointer-events', 'none');

		if (current == "index" && playing) {
			gainNode.gain.linearRampToValueAtTime(0.0, audioCtx.currentTime + 1);
		}

		setTimeout(function () {
			window.location.href = url;
		}, 300);

	} else {
		$('body').addClass('browsing');

		if (current == "index" && playing) {
			gainNode.gain.linearRampToValueAtTime(0.0, audioCtx.currentTime + 0.8);
		}

		setTimeout(function () {
			window.location.href = url;
		}, 400)
	}




}

function autosize($pic) {
	viewport_height = $pic.parent().height()
	viewport_width = $pic.parent().width()
	screen_ratio = viewport_width / viewport_height;
	pic_ratio = $pic.width() / $pic.height();


	if (pic_ratio > screen_ratio) {


		$pic.css({
			height: viewport_height,
			width: Math.round(viewport_height * pic_ratio),
			marginLeft: Math.round(-(viewport_height * pic_ratio - viewport_width) / 2),
			marginTop: 0
		})
	} else {


		$pic.css({
			width: viewport_width,
			height: Math.round(viewport_width / pic_ratio),
			marginTop: Math.round(-(viewport_width / pic_ratio - viewport_height) / 2),
			marginLeft: 0
		})
	}
};


function openMenu() {
	$('#main_menu').removeClass('hidden');
	mainMenuOpen = true;
	setTimeout(function () {
		$('#main_menu .panel_logo .logo_pieces .logo_p._1').removeClass('top_single');
	}, 450)
	setTimeout(function () {
		$('#main_menu .panel_logo .logo_pieces .logo_p._2').removeClass('top_single');
	}, 550)
	setTimeout(function () {
		$('#main_menu .panel_logo .logo_pieces .logo_p._3').removeClass('top_single');
	}, 650);


	setTimeout(function () {
		$('#main_menu .panel_logo .logo_square .border').removeClass('no_width').removeClass('no_height');

	}, 1000)

	setTimeout(function () {
		$('#main_menu .panel_logo .logo_pieces .logo_p._4').removeClass('top_single');

	}, 900)
	setTimeout(function () {
		$('#main_menu .panel_logo .logo_pieces .logo_p._5').removeClass('top_single');

	}, 1000);

	$('#main_menu .panel_logo .star').each(function (s) {
		setTimeout(function () {
			$('#main_menu .panel_logo .star:eq(' + s + ')').removeClass('hidden_by_scaling_full');
		}, 1100 + (100 * s));
	});


	setTimeout(function () {
		$('#main_menu .panel_close .x_close .line_1').removeClass('no_width');
		$('#main_menu .panel_close .x_close .line_2').removeClass('no_height');
		$('#main_menu .panel_close .button_text').removeClass('top_single');
	}, 300);

	setTimeout(function () {
		$('#menu_book .white_button_separator').removeClass('no_width');
	}, 500)

	setTimeout(function () {
		$('#menu_book p._1').removeClass('top_hidden');
		$('#menu_book p._2').removeClass('bottom_hidden');

	}, 700)


	$('.menu_section').each(function (i) {
		setTimeout(function () {
			$('.menu_section:eq(' + i + ') .macro p,.menu_section:eq(' + i + ') .macro a').removeClass('top_hidden');
		}, 500 + (100 * i))
	});

	$('.menu_side_button').each(function (i) {
		setTimeout(function () {
			$('.menu_side_button:eq(' + i + ')').removeClass('top_single')
		}, 1000 + (50 * i));
	})

	setTimeout(function () {
		$('.menu_trail').removeClass('no_height');
	}, 400)

}

function close_menu() {
	mainMenuOpen = false;
	clearMenu();

	setTimeout(function () {
		$('#main_menu').addClass('hidden');
	}, 400)


}

function clearMenu() {
	$('#menu_container').trigger('mouseleave');
	$('.menu_section .macro p,.menu_section .macro a').addClass('top_hidden');
	$('.menu_side_button').addClass('top_single');
	$('#menu_book p._1').addClass('top_hidden');
	$('#menu_book p._2').addClass('bottom_hidden');
	$('#menu_book .white_button_separator').addClass('no_width');
	$('#main_menu .panel_close .x_close .line_1').addClass('no_width');
	$('#main_menu .panel_close .x_close .line_2').addClass('no_height');
	$('#main_menu .panel_close .button_text').addClass('top_single');
	$('.menu_trail').addClass('no_height');
	$('#main_menu .panel_logo .logo_pieces .logo_p._1').addClass('top_single');

	$('#main_menu .panel_logo .logo_pieces .logo_p._2').addClass('top_single');

	$('#main_menu .panel_logo .logo_pieces .logo_p._3').addClass('top_single');
	$('#main_menu .panel_logo .logo_square .border.top,  #main_menu .panel_logo .logo_square .border.bottom').addClass('no_width');
	$('#main_menu .panel_logo .logo_square .border.left, #main_menu .panel_logo .logo_square .border.right').addClass('no_height');


	$('#main_menu .panel_logo .logo_pieces .logo_p._4').addClass('top_single');

	$('#main_menu .panel_logo .logo_pieces .logo_p._5').addClass('top_single');

	$('#main_menu .panel_logo .star').addClass('hidden_by_scaling_full');


}

var submenuIsOpen = false;

function showSubmenu() {

	$subMenu = $(this).next();
	$section = $(this).parent();
	$macro = $(this);




	if (submenuIsOpen) {
		if (!$section.hasClass('active')) {
			hideSubmenu();
		}
	}

	$subMenu = $(this).next();
	$section = $(this).parent();
	$macro = $(this);

	submenuIsOpen = true;





	$section.addClass('active').siblings().addClass('greyed');
	$('.submenu_item', $subMenu).each(function (i) {
		setTimeout(function () {
			$('.submenu_item:eq(' + i + ')', $subMenu).addClass('visible');
		}, 150 + (50 * i));
	});

	setTimeout(function () {
		$section.addClass('auto_height');
	}, 0)



}

function hideSubmenu() {
	submenuIsOpen = false;



	setTimeout(function () {
		$('.menu_section').removeClass('auto_height');
	}, 0)



	$subMenu = $('.menu_section.active .detail');
	$macro = $('.menu_section.active .macro');
	$parent = $('.menu_section.active');



	$('.submenu_item').removeClass('visible');


	$('.menu_section').removeClass('active').removeClass('greyed');
}

function hoverSubmenuItem() {
	$(this).siblings().addClass('greyed');
}

function outSubmenuItem() {
	if ($('.submenu_item.active', $(this).parent()).length == 0) {
		$('.submenu_item', $(this).parent()).removeClass('greyed');
	} else {
		$('.submenu_item:not(.active)', $(this).parent()).addClass('greyed');

	}
}

function showSquareLabel(container) {
	$('.big_square', container).removeClass('hidden_by_scaling_low');
	setTimeout(function () {
		$('.big_square .inline_back', container).removeClass('no_width');
	}, 100);
	setTimeout(function () {
		$('.big_square .square_label', container).removeClass('top_single');
	}, 400)
}




function openBookPanel() {
	$('#book_panel').removeClass('hidden');
	bookPanelOpen = true;



	setTimeout(function () {
		$('#book_panel .panel_close .x_close .line_1').removeClass('no_width');
		$('#book_panel .panel_close .x_close .line_2').removeClass('no_height');
		$('#book_panel .panel_close .button_text').removeClass('top_single');
	}, 300);

	setTimeout(function () {
		$('#book_panel .arrival label').removeClass('top_double');
	}, 300)

	setTimeout(function () {
		$('#book_panel .arrival input').removeClass('top_double');
	}, 400)
	setTimeout(function () {
		$('#book_panel .departure label').removeClass('top_double');
	}, 400)

	setTimeout(function () {
		$('#book_panel .departure input').removeClass('top_double');
	}, 500)
	setTimeout(function () {
		$('#book_panel .people label').removeClass('top_double');
	}, 500)

	setTimeout(function () {
		$('#book_panel .people input').removeClass('top_double');
	}, 600)

	setTimeout(function () {
		$('#book_panel #book_submit .submit').removeClass('top_double');
	}, 700)


}

function closeBookPanel() {
	bookPanelOpen = false;


	$('#book_panel .panel_close .x_close .line_1').addClass('no_width');
	$('#book_panel .panel_close .x_close .line_2').addClass('no_height');
	$('#book_panel .panel_close .button_text').addClass('top_single');



	$('#book_panel .arrival label').addClass('top_double');



	$('#book_panel .arrival input').addClass('top_double');


	$('#book_panel .departure label').addClass('top_double');



	$('#book_panel .departure input').addClass('top_double');

	$('#book_panel .people label').addClass('top_double');

	$('#book_panel .people input').addClass('top_double');

	$('#book_panel #book_submit .submit').addClass('top_double');

	setTimeout(function () {
		$('#book_panel').addClass('hidden');
	}, 300)



}



/********************** MAILER ****************/




function mailer() {
	$scope = $(this).parent().parent().parent()


	full_lang = get_extendend_lang();
	page = $('#section_title h2').text();


	if (current == "rooms") {
		page = "Camera " + $('#section_title h2').text();
	}




	if (current == "spa") {
		page = "Stai SPA"
	}

	if (current == "specials") {
		page = "All Seasons Promotions:  " + $('.title_section h3').text();
	}

	form_type = get_form_type();


	controller = form_type.type;
	subject = form_type.request;

	name = $('#name').val().toLowerCase().replace(/\b[a-z]/g, function (letter) {
		return letter.toUpperCase();
	});
	surname = $('#surname').val().toLowerCase().replace(/\b[a-z]/g, function (letter) {
		return letter.toUpperCase();
	});



	email = $('#email').val();
	phone = $('#phone_c').val();
	mobile = $('#mobile').val();
	message = $('#message').val();

	arrival = $('#form_arrival').val();
	departure = $('#form_departure').val();
	rooms = $('#form_rooms').val();
	guests = $('#form_guests').val();
	time = $('#time').val();

	wedding_date = $('#wedding_date').val();
	alt_wedding_date = $('#alt_wedding_date').val();
	wed_people = $('#wed_people').val();
	event_type = $('#event_type').val();

	budget = $('#budget').val();
	buffet_location = $('#buffet_location').val();
	dining_location = $('#dining_location').val();

	restaurant_date = $('#restaurant_date').val();

	company = $('#company').val();
	social_reg = $('#social_reg').val();
	company_ref = $('#ref').val();


	prv = $('#prv');



	var email_reg_exp = /^([a-zA-Z0-9_\.\-])+\@(([a-zA-Z0-9\-]{2,})+\.)+([a-zA-Z0-9]{2,})+$/;

	validating = true;

	$('.required', $scope).each(function () {
		if ($(this).val() == "" || $(this).val() == undefined || $(this).val() == null) {
			validating = false;
		}
	})

	if (!validating) {
		switch (lang) {
			case 'en':
			case 'de':
				alert('Please fill all required fields!');
				break;
			case 'it':
				alert('Si prega di compilare tutti i campi richiesti, grazie!');
				break;
			case 'fr':
				alert('S\'il vous plaÃ®t, remplir tous les champs obligatoires, merci!');
				break;

		}
		return false;
	}



	if (!email_reg_exp.test(email)) {
		switch (lang) {
			case 'en':
			case 'de':
				alert('Please provide a valid email address!');
				break;
			case 'it':
				alert('Si prega di inserire una mail valida grazie!');
				break;
			case 'it':
				alert('S\'il vous plaÃ®t, entrer une adresse email valide, merci!');
				break;

		}
		$("#email").focus();
		return false;
	}

	if (message == '') {
		switch (lang) {
			case 'en':
			case 'de':
				alert('Please insert a message!');
				break;
			case 'it':
				alert('Si prega di inserire il messaggio!');
				break;
			case 'fr':
				alert('S\'il vous plaÃ®t entrer dans le message!');
				break;

		}
		$("#message").focus();
		return false;
	}

	if (!prv.prop('checked')) {
		switch (lang) {
			case 'en':
			case 'de':
				alert('You should accept the privacy policy in order to contact us!');
				break;
			case 'it':
				alert('Si prega di accettare i termini della privacy policy per poterci contattare!');
				break;
			case 'fr':
				alert('S\'il vous plaÃ®t, accepter les termes de la privacy policy pour nous contacter!');
				break;

		}
		return false;
	}

	vars = $.param({
		"site_lang": full_lang,
		"controller": controller,
		"subject": subject,
		"page": page,
		"name": name,
		"surname": surname,
		"phone": phone,
		"mobile": mobile,
		"email": email,
		"message": message,
		"arrival": arrival,
		"departure": departure,
		"rooms": rooms,
		"guests": guests,
		"time": time,
		"wedding_date": wedding_date,
		"alt_wedding_date": alt_wedding_date,
		"wed_people": wed_people,
		"event_type": event_type,
		"budget": budget,
		"buffet_location": buffet_location,
		"dining_location": dining_location,
		"restaurant_date": restaurant_date,
		"company": company,
		"social_reg": social_reg,
		"company_ref": company_ref
	});


	$.ajax({
		type: "POST",
		url: "/" + lang + "/mailer",
		data: vars,
		dataType: "json",
		success: function (msg) {
			if (msg.message == '0') {
				switch (lang) {
					case 'en':
					case 'de':
						alert('Thank you! Your request has been submitted and we will contact you as soon as we can!');
						break;
					case 'it':
						alert('Grazie! La Vostra richiesta Ã¨ stata inviata, Vi risponderemo appena possibile!');
						break;
					case 'fr':
						alert('Merci! Votre demande a Ã©tÃ© envoyÃ©e et nous vous rÃ©pondrons dÃ¨s que possible!');
						break;
				}
				$('input[type!="submit"][type!="reset"]').each(function () {
					/*if($(this).attr('id')!='prv'){
						$(this).val('');
					};*/
				});

				$('#prv').prop('checked', false);

			} else if (msg.message == '1') {
				switch (lang) {
					case 'en':
					case 'de':
						alert('We are sorry, but there was a problem while processing your request, please try again later, thank you!');
						break;
					case 'it':
						alert('Nous sommes dÃ©solÃ©s mais il ya eu une erreur dans le traitement de votre demande, s\'il vous plaÃ®t essayer de nouveau plus tard, merci');
						break;

				}

			}
		}
	});
}

function get_extendend_lang() {
	var ext_lang;


	switch (lang) {
		case 'it':
			ext_lang = 'italiano';
			break;
		case 'en':
			ext_lang = 'inglese';
			break;
		case 'fr':
			ext_lang = 'francese';
			break;
		case 'de':
			ext_lang = 'tedesco';
			break;
	}


	return ext_lang;
}



function get_form_type() {
	if (current == "rooms" && action == "detail") {
		return {
			type: "full_date",
			request: "richiesta di prenotazione per una " + page
		}
	} else if (current == "events") {
		return {
			type: "events",
			request: "richiesta di informazioni riguardo un evento"
		}
	} else if (current == "weddings") {
		return {
			type: "weddings",
			request: "richiesta di informazioni per una cerimonia"
		}
	} else if (current == "dining") {
		return {
			type: "dining",
			request: "richiesta di informazioni per prenotare un tavolo al Ristorante"
		}
	} else if (current == "spa") {
		return {
			type: "spa",
			request: "richiesta di informazioni tramite form SPA"
		}
	} else if (current == "facilities") {
		return {
			type: "default",
			request: "richiesta di informazioni sul noleggio di " + page
		}
	} else if (current == "specials") {
		return {
			type: "specials",
			request: "richiesta di informazioni sull'offerta speciale " + $('.title_section:eq(0) h3').text()
		}
	} else {
		return {
			type: "default",
			request: "richiesta generica"
		};
	}

}






/************** BOOKING ****************/


$('.submit').bind('click', function () {
	switch (lang) {
		case 'it':
			booking_lang = 2;
			break;
		case 'en':
			booking_lang = 1;
			break;
		case 'fr':
			booking_lang = 4;
			break;
		case 'de':
			booking_lang = 3;
			break;
	}

	if (mobileCheck) {
		current_guests = $('#mobile_guests').val();
		current_rooms = 1;
		if (lang == 'en') {
			window.open('https://secure.ermeshotels.com/bol_mobile/dispo.do?CA=367&hotel=1773&lang=' + booking_lang + '&dataDa=' + ("0" + (arrivalDate.getMonth() + 1)).slice(-2) + '/' + ("0" + arrivalDate.getDate()).slice(-2) + '/' + arrivalDate.getFullYear() + '&dataA=' + ("0" + (departureDate.getMonth() + 1)).slice(-2) + '/' + ("0" + departureDate.getDate()).slice(-2) + '/' + departureDate.getFullYear() + '&num_cam=' + current_rooms + '&num_ad=' + $('#guests').val())

		} else {
			window.open('https://secure.ermeshotels.com/bol_mobile/dispo.do?CA=367&hotel=1773&lang=' + booking_lang + '&dataDa=' + ("0" + arrivalDate.getDate()).slice(-2) + '/' + ("0" + (arrivalDate.getMonth() + 1)).slice(-2) + '/' + arrivalDate.getFullYear() + '&dataA=' + ("0" + departureDate.getDate()).slice(-2) + '/' + ("0" + (departureDate.getMonth() + 1)).slice(-2) + '/' + departureDate.getFullYear() + '&num_cam=' + current_rooms + '&num_ad=' + $('#guests').val())

		}
	} else {
		if (lang == 'en') {
			window.open('https://secure.ermeshotels.com/bol/dispo.do?caId=367&hoId=1773&lang=' + booking_lang + '&dataDa=' + ("0" + (arrivalDate.getMonth() + 1)).slice(-2) + '/' + ("0" + arrivalDate.getDate()).slice(-2) + '/' + arrivalDate.getFullYear() + '&dataA=' + ("0" + (departureDate.getMonth() + 1)).slice(-2) + '/' + ("0" + departureDate.getDate()).slice(-2) + '/' + departureDate.getFullYear() + '&num_ad=' + $('#guests').val())

		} else {
			window.open('https://secure.ermeshotels.com/bol/dispo.do?caId=367&hoId=1773&lang=' + booking_lang + '&dataDa=' + ("0" + arrivalDate.getDate()).slice(-2) + '/' + ("0" + (arrivalDate.getMonth() + 1)).slice(-2) + '/' + arrivalDate.getFullYear() + '&dataA=' + ("0" + departureDate.getDate()).slice(-2) + '/' + ("0" + (departureDate.getMonth() + 1)).slice(-2) + '/' + departureDate.getFullYear() + '&num_ad=' + $('#guests').val())

		}
	}
})





/****** METEO **********/

$.get("http://api.openweathermap.org/data/2.5/forecast/daily?id=6539991&mode=xml&units=metric&APPID=f671ceaeb6c57fd350ac555ea864d9e2", function (data) {

	weatherData = data;
	$icon = $(weatherData).find('time:first-child symbol').attr('var');
	$temp = $(weatherData).find('time:first-child temperature').attr('day');
	$('#widget_meteo .weather_img').html('<img class="full_width" src="/images/weather/' + $icon + '.png" />');
	$('#widget_meteo .temp_label span').html(Math.round($temp));
});

function run_clocks() {


	var time;

	function updateClock() {
		time = moment().tz('Europe/Rome'),
			second = time.seconds() * 6,
			minute = time.minutes() * 6 + second / 60,
			hour = ((time.hours() % 12) / 12) * 360 + minute / 12;

		$('#clock .hours').css("transform", "translateY(0.5px) rotateZ(" + hour + "deg)");
		$('#clock .minutes').css("transform", "rotateZ(" + minute + "deg)");



	}

	function timedUpdate() {
		updateClock();
		clocks_running = setTimeout(timedUpdate, 60000);
	}

	timedUpdate();

	$nowDate = new Date();
	$nowHour = $nowDate.getHours();

	$('#widget_date .year').text($nowDate.getFullYear());
	if (lang == "en") {
		$('#widget_date .day').text($nowDate.getDate() + '' + nth($nowDate.getDate()));
	} else {
		$('#widget_date .day').text($nowDate.getDate());

	}


	$('#widget_date .month').text(getMonthNames()[($nowDate.getMonth())].substr(0, 3));

}



function nth(d) {
	if (d > 3 && d < 21) return 'th';
	switch (d % 10) {
		case 1:
			return "st";
		case 2:
			return "nd";
		case 3:
			return "rd";
		default:
			return "th";
	}
}


var mesi = new Array();
mesi[0] = "Gennaio";
mesi[1] = "Febbraio";
mesi[2] = "Marzo";
mesi[3] = "Aprile";
mesi[4] = "Maggio";
mesi[5] = "Giugno";
mesi[6] = "Luglio";
mesi[7] = "Agosto";
mesi[8] = "Settembre";
mesi[9] = "Ottobre";
mesi[10] = "Novembre";
mesi[11] = "Dicembre";



var month = new Array();
month[0] = "January";
month[1] = "February";
month[2] = "March";
month[3] = "April";
month[4] = "May";
month[5] = "June";
month[6] = "July";
month[7] = "August";
month[8] = "September";
month[9] = "October";
month[10] = "November";
month[11] = "December";

var mois = new Array();
mois[0] = "Janvier";
mois[1] = "FÃ©vrier";
mois[2] = "Mars";
mois[3] = "Avril";
mois[4] = "Mai";
mois[5] = "Juin";
mois[6] = "Juillet";
mois[7] = "AoÃ»t";
mois[8] = "Septembre";
mois[9] = "Octobre";
mois[10] = "Novembre";
mois[11] = "DÃ©cembre";

var monate = new Array();
monate[0] = "Januar";
monate[1] = "Februar";
monate[2] = "MÃ¤rz";
monate[3] = "April";
monate[4] = "Mai";
monate[5] = "Juni";
monate[6] = "Juli";
monate[7] = "August";
monate[8] = "September";
monate[9] = "Oktober";
monate[10] = "November";
monate[11] = "Dezember";

var giorni = new Array();
giorni[1] = "Lun";
giorni[2] = "Mar";
giorni[3] = "Mer";
giorni[4] = "Gio";
giorni[5] = "Ven";
giorni[6] = "Sab";
giorni[0] = "Dom";


var jours = new Array();
jours[0] = "Lun";
jours[1] = "Mar";
jours[2] = "Mer";
jours[3] = "Jeu";
jours[4] = "Ven";
jours[5] = "Sam";
jours[6] = "Dim";

var tage = new Array();
tage[0] = "Mo";
tage[1] = "Di";
tage[2] = "Mi";
tage[3] = "Do";
tage[4] = "Fr";
tage[5] = "Sa";
tage[6] = "So";


function getMonthNames() {
	if (lang == "it") {
		return mesi;
	} else if (lang == "fr") {
		return mois;
	}
	if (lang == "de") {
		return monate;
	} else {
		return month
	}
}



function getDowNames() {
	if (lang == "it") {
		return giorni;
	} else if (lang == "fr") {
		return jours;
	} else if (lang == "de") {
		return tage;
	} else {
		return null
	}
}

function getDowOffset() {
	if (lang == "it") {
		return 1;
	} else if (lang == "fr") {
		return 1;
	} else if (lang == "de") {
		return 1;
	} else {
		return 0
	}
}

function addDays(date, days) {
	var result = new Date(date);
	result.setDate(result.getDate() + days);
	return result;
}

function get_locale() {
	if (lang == "it") {
		return "it-IT"
	} else if (lang == "en") {
		return "en-US"
	} else if (lang == "fr") {
		return "fr-FR"
	}
}

function set_lang(e) {
	e.preventDefault();
	current_url = window.location.href;
	if (current_url.indexOf("/" + lang + "/") == -1) {
		window.location = "/" + $(this).attr("rel") + "/home"
	} else {
		new_url = current_url.replace("/" + lang + "/", "/" + $(this).attr("rel") + "/");
		window.location = new_url
	}
}