'use strict';

(function () {
	var lastTime = 0;
	var vendors = ['webkit', 'moz'];
	for (var x = 0; x < vendors.length && !window.requestAnimationFrame; ++x) {
		window.requestAnimationFrame = window[vendors[x] + 'RequestAnimationFrame'];
		window.cancelAnimationFrame = window[vendors[x] + 'CancelAnimationFrame'] || window[vendors[x] + 'CancelRequestAnimationFrame'];
	}

	if (!window.requestAnimationFrame) window.requestAnimationFrame = function (callback, element) {
		var currTime = new Date().getTime();
		var timeToCall = Math.max(0, 16 - (currTime - lastTime));
		var id = window.setTimeout(function () {
			callback(currTime + timeToCall);
		}, timeToCall);
		lastTime = currTime + timeToCall;
		return id;
	};

	if (!window.cancelAnimationFrame) window.cancelAnimationFrame = function (id) {
		clearTimeout(id);
	};
})();
$(function () {
	var speed = 60;
	var rangeMin = 10;
	var rangeMax = 20;
	var min = 50;
	var max = 100;
	var speedUp = true;
	var speedUpNum = 2;
	var speedUpRange = 30;
	var speedUpMax = 180;
	var music = $('#player')[0];
	var musicClose;
	var musicOpen = 'data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAD0AAAA8CAMAAADi4EJ+AAAA1VBMVEUAAAD5uRr3uBr7uRriqh37uhr7uhr7uhrxtBv6uRr7uhr7uhr7uhr7uhr7uhr7uhr7uhr7uhr6uRr7uhr7uhr7uhr6uhr6uhr7uhr7uhr7uhr6uRr7uhrytRv7uhr7uhr2txr7uhr6uhqMeSbmrhzssRzWpB5mYioCKDWihiTGmyCkhyMCKDX7uhoqPzHToh4MLjNIUS2ujCLPoB/wsxsgOjJbXCvbpx30thvInB8YNTLkrRxoZCp6biidgyRDTi41Ri89Si5RViyHdiaoiSO9liGQeyVJMoGDAAAALHRSTlMAEczWA5GpyiAYCEW6DfUm7+HEnX9mVDUu6LF3bNmKXjzQTB7j/vLlEpiSeGoGoYYAAAMOSURBVEjHnddpc6owFAZgKG4givu+X3vXvKJFBfely///STcnMoy1FiHvp850HpN4kpOoREimNyzVEopUEoMheEZS9ve/Z1BSMoP//nPBSKsSQ//1gM2rLaVzSXC8XDgyWk+BZ8smMmNnSoS9g9BPMXW9y+3K8WYyetTk+GWyh4yu5gkv2IuETlTAs5+yqYQWlcLHlMloUSlvx1h0nQhimRzPt+yrTpqVmmV8LW2t0kv66Wlig7E7mjZAqVL/7PV2F5/jntk93UlTGYsV6woPnnAT+8RudZq0YWUraRo/G+Bs4Ravjuy+plidIdDs+8e90STgvJ4PhxnPYenOTyxEc1EG8lXBc1Sd5x3fF9Mpo6yd+SRcK2qb85qYN5WWH8Ldi+OIMRePtWJ0AK1OJeT6dbqbA3N39kgP6obPe0BZFSWcH5fw9ssDoXDdKpZrF58xAT53DbCP79jSoh9qTvLJjOC1PFI66dXZtQmH6icx82QTaFn+WchnhV66TjSdyA1MoGwQrwLtWJpilZDvix1aRCq2Fgumv3UTWnytt6DRytUymhe9caPrXA+FLJU8iSJp5/SxY9LanSzWkTWVSmvQp/gzd2bvb5FnTge6pYr1B9+aHVlnWkBVXDUaWkKfN05EbTRSoILx9IGKMgQ2x+2SRdKNtgakG8pl2cgqLXHHsmiaOklKYGVUoFNS9dtvkHWINgtmRw0uDN7b9DS1/v2SV01kerSfv9Wjke53hwr40FSBPHi8jX2JY3vz2Tc6SKIKNLOXAhZxE3cWrnOdAkBNlWKVCzd8/itMW9QL20YwkXo79dTtdrWh1h2Px7QQ4vd1ploC8u2cchVDzwQZ0VJ293RCH7S5hdY3Qt47GvE7um8WQY2xHvrsbKRFl7/WXb+nopjMqo+eaqZ4tlzpMZGyVu7U1WiPD+KB/vkj9hP1bc2kNG1G4ou9r514Wkl0OH9feTKa0r+8ciW1UqMtvJRZd3CC7NPCERWLn8YQcLer+O/z4NaD5wUvD7mfFzANRSpqknRHkYxR1Qq93L3//AeURClLonNougAAAABJRU5ErkJggg==';
	var mask = $('.jiu');
	var jiu = $('.jl');
	var pm = $('.pm');
	var jdMask = $('.jd-mask');
	var line = $('.valid');
	var jdLength = 3.6;
	var btn = $('.anniu');
	var numEl = $('.number');
	var num = -1;
	var result = $('.result');
	var title = $('.title');
	var dj = $('.dj');
	var djImg = $('.dj-img');

	var caoSafari = setInterval(function () {
		if (title.hasClass('safari')) {
			title.removeClass('safari');
		} else {
			title.addClass('safari');
		}
	}, 1000);
	var caoSafari2;
	function daojiu(state) {
		if (state == 1) {
			caoSafari2 = setInterval(function () {
				if (djImg.hasClass('safari')) {
					djImg.removeClass('safari');
				} else {
					djImg.addClass('safari');
				}
			}, 200);
		} else {
			clearInterval(caoSafari2);
		};
	}
	var stbtn = $('.stbtn');
	var jiubei = $('.jiubei');
	var animate = function animate(num) {
		var fixNum = num * 0.736;
		var i = fixNum / 100;
		var o = fixNum > 0 ? 100 / fixNum : 100;
		var p = -3.6 * (num / 100) + 'rem';
		var jd = 100 - num;
		if (!jiu.hasClass('ani')) {
			jiu.addClass('ani');
		};
		pm.css({
			'-webkit-transform': 'translateY(' + p + ')',
			'transform': 'translateY(' + p + ')'
		});
		mask.css({
			'-webkit-transform': 'scaleY(' + i + ')',
			'transform': 'scaleY(' + i + ')'
		});
		jiu.css({
			'-webkit-transform': 'scaleY(' + o + ')',
			'transform': 'scaleY(' + o + ')'
		});
		jdMask.css({
			'background-image': '-webkit-linear-gradient(top, #15566f, #15566f ' + jd + '%, transparent ' + jd + '%, transparent);'
		});
		return function () {};
	};
	var game = new Game({
		el: '.anniu',
		speed: speed,
		drow: animate
	});
	function init() {
		$(game).trigger('start');
		setTimeout(function () {
			$('.pop').addClass('active');
			$('.start').addClass('ani');
		}, 2000);
	}
	function changeMusic() {
		var icon = $('#music-icon');
		if (icon.hasClass('music-n')) {
			music.play();
			icon.removeClass('music-n').addClass('music-y');
			musicClose = icon.attr('src');
			icon.prop('src', musicOpen);
		} else {
			music.pause();
			icon.removeClass('music-y').addClass('music-n');
			icon.prop('src', musicClose);
		};
	}
	stbtn.on('click', function () {
		changeMusic();
		startGame();
	});
	$('#music-icon').on('click', function () {
		changeMusic();
	});
	function startGame() {
		$('.pop').removeClass('active');
		$('.title').addClass('out');
		$('.logo').hide();
		$('.start').addClass('hide');
		clearInterval(caoSafari);
		fcJiubei();
	}
	function speedUpFun(speedUpNum, speedUpRange, speedUpMax) {
		var lv = Math.floor(num / speedUpNum);
		var sp = lv * speedUpRange + speed;
		if (sp > speedUpMax) {
			sp = speedUpMax;
		};
		return sp;
	}
	function fcJiubei() {
		jiubei.addClass("out");
		num++;
		numRich(num);
		if (speedUp) {
			game.speed = speedUpFun(speedUpNum, speedUpRange, speedUpMax) || speed;
		};
	}
	jiubei.on('webkitTransitionEnd', function () {
		if ($(this).hasClass('out')) {
			gameInit();
			jiubei.removeClass("out");
		} else {
			if (num == 0) {
				$('.tips').addClass('ani');
			};
			$('.count').removeClass('hide');
			$('.jindu,.xian,.anniu').removeClass('hide');
		}
	});
	dj.on('webkitTransitionEnd', function (e) {
		e.stopPropagation();
	});
	function gameInit() {
		animate(0);
		console.log(game.speed);
		pm.removeClass('ani');
		btn.removeClass('an1');
		$('.outof').hide();
		$('.fail img,.fail div').removeClass('ani');
		initLine();
	}
	function initLine() {
		var range = game.random(min, max, rangeMin, rangeMax);
		var l = (range[1] - range[0]) * jdLength / 100;
		var t = (100 - range[1]) * jdLength / 100;
		line.css({
			'-webkit-transform': 'translateY(' + t + 'rem)',
			'transform': 'translateY(' + t + 'rem)',
			'height': l + 'rem'
		});
	}
	function validate(state, failResult) {
		if (state == true) {
			line.addClass('sani');
		} else {
			if (failResult > 0) {
				$('.fail-title').html('倒多了倒多了点，充字数冲冲');
			} else {
				$('.fail-title').html('才喝这么一点，太没诚意了吧');
			};
			line.addClass('fani');
		};
	}

	function numRich(num) {
		numEl.html(num);
	}
	gameInit();
	line.on('webkitAnimationEnd', function () {
		if (line.hasClass('sani')) {
			line.removeClass('sani');
			fcJiubei();
		} else {
			setTimeout(function () {
				result.html(num + '瓶');
				$('.pop').addClass('active');
				if (num <= 0) {
					$('.over-main.has-one').addClass('hide');
					$('.over-main.no-one').removeClass('hide');
					$('.xy').addClass('hide');
					$('.cw').addClass('no-one');
				} else {
					$('.over-main.no-one').addClass('hide');
					$('.over-main.has-one').removeClass('hide');
					$('.cw').removeClass('no-one');
					$('.xy').removeClass('hide');
				};
				$('.game-over').addClass('ani');
				line.removeClass('fani');
			}, 700);
			$('.fail img,.fail div').addClass('ani');
		}
	});
	$('.cw').on('click', function () {
		num = -1;
		$('.pop').removeClass('active');
		$('.game-over').removeClass('ani').addClass('hide');
		fcJiubei();
	});
	$('.xy').on('click', function () {
		$('.share').show();
	});
	$('.share').on('click', function () {
		$('.share').hide();
	});
	$(game).on('game:start', function () {
		pm.addClass('ani');
		btn.addClass('an1');
		$('.tips').removeClass('ani');
		if (num >= 0) {
			dj.addClass('ani');
			daojiu(1);
		}
	});
	// $(game).on('start',function(){
	// 	aaa()
	// })
	$(game).on('game:update', function (e, data) {
		// console.log(data);
		// animate(data);
	});
	$(game).on('game:over', function () {
		if (num >= 0) {
			animate(120);
			$('.outof').show();
		};
	});
	$(game).on('game:end', function (e, data) {
		var range = game.randomNum;
		dj.removeClass('ani');
		daojiu(0);
		btn.addClass('hide');
		if (data <= range[1] && data >= range[0]) {
			validate(true);
		} else if (data > range[1]) {
			validate(false, 1);
		} else {
			validate(false, -1);
		};
	});
	init();
});