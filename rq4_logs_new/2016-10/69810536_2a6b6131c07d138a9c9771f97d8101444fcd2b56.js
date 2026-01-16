$(document).ready(function() {

	// Ajax
	$('#callback_1').submit(function() {
		$.ajax({
			type: 'POST',
			url: 'https://formspree.io/vetalsd2@gmail.com',
			dataType: 'json,',
			data: $(this).serialize()
		}).done(function(){
			$('.message_result').fadeIn('fast');
		});
		return false;
	});

	// Tabs for sliders
	$('.tab_content>.carousel_tab:first').show();

	$('.tab_control>li').each(function(i){
		$(this).attr('data-tab', i);
	});

	$('.tab_content>.carousel_tab').each(function(i){
		$(this).attr('data-tab', i);
	});

		$('.tab_control>li').click(function(){
			$('.tab_control>li').removeClass('active_li');
			$(this).addClass('active_li');

			var thisTab = $(this).attr('data-tab');
			$('.tab_content>.carousel_tab').hide();
			$('.tab_content>.carousel_tab[data-tab='+thisTab+']').show();
		});


	// Sliders
	$('.chair_carousel').slick({
		autoplay: false,
		prevArrow: '.prev',
		nextArrow: '.next',
		speed: 700,
		arrows: true,
		dots: false
	});

	$('.carousel_for_adult').slick({
		autoplay: false,
		prevArrow: '.main_prevv',
		nextArrow: '.main_nextt',
		speed: 700,
		arrows: true,
		dots: false,
		slidesToShow: 3,
		slidesToScroll: 1,
		responsive: [{
			breakpoint: 768,
			settings: {
				slidesToShow: 1,
				slidesToScroll: 1,
			}
		}]
	});

	$('.carousel_for_children').slick({
		autoplay: true,
		prevArrow: '.main_prev',
		nextArrow: '.main_next',
		speed: 700,
		arrows: true,
		dots: false,
		slidesToShow: 3,
		slidesToScroll: 1,
		responsive: [{
			breakpoint:768,
			settings: {
				slidesToShow: 1,
				slidesToScroll: 1,
			}
		}]
	});
	// Feedback Buttons
	$('.callback_button').click(function(){
		$('.callback_form').fadeIn();
	});
	$('.find_out_button').click(function(){
		$('.find_out_callback').fadeIn();
	});
	$('.pick_up_button').click(function(){
		$('.pick_up_callback').fadeIn();
	});

	$('.close').click(function(){
		$('.form_callback').fadeOut();
	});

	// Вывод счетчика текущих слайдов не удался "1/3"
	$('.header_chair').each(function(i){
		var thisAttr = $(this).attr('title', ++i );
		var thisS = $('slick-current').attr('title');
		console.log();
		$('.counter>span').appendTo('<span>thisAttr</span>');
	});
	// Validation
	$('#callback_1').validate({
		rules: {
			name: {
				required: true,
				minlength: 2,
				maxlength: 20
			},
			email: {
				required: true,
				email: true
			},
			phone: {
				required: true,
				digits: true
			}
		},
		messages: {
			name: {
				required: "Это поле обязательно для заполнения",
				minlength: "Имя должно вмещать минимум 2 буквы",
				maxlength: "Имя должно вмещать максимум 20 букв"
			},
			email: {
				required: "Это поле обязательно для заполнения",
				email: "Введите корректный email"
			},
			phone: {
				required: "Это поле обязательно для заполнения",
				digits: "Номер не может содержать буквы"
			}
		}
	});

	$('#callback_2').validate({
		rules: {
			name: {
				required: true,
				minlength: 2,
				maxlength: 20
			},
			email: {
				required: true,
				email: true
			},
			phone: {
				required: true,
				digits: true
			}
		},
		messages: {
			name: {
				required: "Это поле обязательно для заполнения",
				minlength: "Имя должно вмещать минимум 2 буквы",
				maxlength: "Имя должно вмещать максимум 20 букв"
			},
			email: {
				required: "Это поле обязательно для заполнения",
				email: "Введите корректный email"
			},
			phone: {
				required: "Это поле обязательно для заполнения",
				digits: "Номер не может содержать буквы"
			}
		}
	});

	$('#callback_3').validate({
		rules: {
			name: {
				required: true,
				minlength: 2,
				maxlength: 20
			},
			email: {
				required: true,
				email: true
			},
			phone: {
				required: true,
				digits: true
			}
		},
		messages: {
			name: {
				required: "Это поле обязательно для заполнения",
				minlength: "Имя должно вмещать минимум 2 буквы",
				maxlength: "Имя должно вмещать максимум 20 букв"
			},
			email: {
				required: "Это поле обязательно для заполнения",
				email: "Введите корректный email"
			},
			phone: {
				required: "Это поле обязательно для заполнения",
				digits: "Номер не может содержать буквы"
			}
		}
	});

});