history.scrollRestoration = "manual";
document.addEventListener("DOMContentLoaded", function() {
	// DETECT TOUCH
	var isTouchDevice = (('ontouchstart' in window)
	         || (navigator.MaxTouchPoints > 0)
	         || (navigator.msMaxTouchPoints > 0));
	if(/(android|bb\d+|meego).+mobile|avantgo|bada\/|blackberry|blazer|compal|elaine|fennec|hiptop|iemobile|ip(hone|od)|ipad|iris|kindle|Android|Silk|lge |maemo|midp|mmp|netfront|opera m(ob|in)i|palm( os)?|phone|p(ixi|re)\/|plucker|pocket|psp|series(4|6)0|symbian|treo|up\.(browser|link)|vodafone|wap|windows (ce|phone)|xda|xiino/i.test(navigator.userAgent) 
	|| /1207|6310|6590|3gso|4thp|50[1-6]i|770s|802s|a wa|abac|ac(er|oo|s\-)|ai(ko|rn)|al(av|ca|co)|amoi|an(ex|ny|yw)|aptu|ar(ch|go)|as(te|us)|attw|au(di|\-m|r |s )|avan|be(ck|ll|nq)|bi(lb|rd)|bl(ac|az)|br(e|v)w|bumb|bw\-(n|u)|c55\/|capi|ccwa|cdm\-|cell|chtm|cldc|cmd\-|co(mp|nd)|craw|da(it|ll|ng)|dbte|dc\-s|devi|dica|dmob|do(c|p)o|ds(12|\-d)|el(49|ai)|em(l2|ul)|er(ic|k0)|esl8|ez([4-7]0|os|wa|ze)|fetc|fly(\-|_)|g1 u|g560|gene|gf\-5|g\-mo|go(\.w|od)|gr(ad|un)|haie|hcit|hd\-(m|p|t)|hei\-|hi(pt|ta)|hp( i|ip)|hs\-c|ht(c(\-| |_|a|g|p|s|t)|tp)|hu(aw|tc)|i\-(20|go|ma)|i230|iac( |\-|\/)|ibro|idea|ig01|ikom|im1k|inno|ipaq|iris|ja(t|v)a|jbro|jemu|jigs|kddi|keji|kgt( |\/)|klon|kpt |kwc\-|kyo(c|k)|le(no|xi)|lg( g|\/(k|l|u)|50|54|\-[a-w])|libw|lynx|m1\-w|m3ga|m50\/|ma(te|ui|xo)|mc(01|21|ca)|m\-cr|me(rc|ri)|mi(o8|oa|ts)|mmef|mo(01|02|bi|de|do|t(\-| |o|v)|zz)|mt(50|p1|v )|mwbp|mywa|n10[0-2]|n20[2-3]|n30(0|2)|n50(0|2|5)|n7(0(0|1)|10)|ne((c|m)\-|on|tf|wf|wg|wt)|nok(6|i)|nzph|o2im|op(ti|wv)|oran|owg1|p800|pan(a|d|t)|pdxg|pg(13|\-([1-8]|c))|phil|pire|pl(ay|uc)|pn\-2|po(ck|rt|se)|prox|psio|pt\-g|qa\-a|qc(07|12|21|32|60|\-[2-7]|i\-)|qtek|r380|r600|raks|rim9|ro(ve|zo)|s55\/|sa(ge|ma|mm|ms|ny|va)|sc(01|h\-|oo|p\-)|sdk\/|se(c(\-|0|1)|47|mc|nd|ri)|sgh\-|shar|sie(\-|m)|sk\-0|sl(45|id)|sm(al|ar|b3|it|t5)|so(ft|ny)|sp(01|h\-|v\-|v )|sy(01|mb)|t2(18|50)|t6(00|10|18)|ta(gt|lk)|tcl\-|tdg\-|tel(i|m)|tim\-|t\-mo|to(pl|sh)|ts(70|m\-|m3|m5)|tx\-9|up(\.b|g1|si)|utst|v400|v750|veri|vi(rg|te)|vk(40|5[0-3]|\-v)|vm40|voda|vulc|vx(52|53|60|61|70|80|81|83|85|98)|w3c(\-| )|webc|whit|wi(g |nc|nw)|wmlb|wonu|x700|yas\-|your|zeto|zte\-/i.test(navigator.userAgent.substr(0,4))) { 
	isTouchDevice = true;
	} else {
	isTouchDevice = false;
	}

	qsa('a[data-scroll]').forEach(el => {
		el.onclick = (e) => {
			e.preventDefault();
			if(window.innerWidth < 768) {
				qs('.header__nav').classList.remove('active');
			}
			let scrollToY = offsetTop(qs(el.dataset.scroll));

			if(el.dataset.scroll == '#about' && window.innerWidth < 768) {
				scrollToY = offsetTop(qs(el.dataset.scroll)) - 60;
			}

			scrollTo(scrollToY);
		}
	})
	const scrollRaf = window.requestAnimationFrame ||
	    window.webkitRequestAnimationFrame ||
	    window.mozRequestAnimationFrame ||
	    window.oRequestAnimationFrame ||
	    window.msRequestAnimationFrame;

	function scrollTo(to) {
	    const start = window.scrollY || window.pageYOffset;
	    const time = Date.now();
	    const duration = Math.abs(start - to) / 2;

	    (function step() {
	        var dx = Math.min(1, (Date.now() - time) / duration)
	        var pos = start + (to - start) * easeInOutCubic(dx)

	        window.scrollTo(0, pos)

	        if (dx < 1) {
	            scrollRaf(step);
	        }
	    })()
	}
	function easeInOutCubic(x) {
		return x < 0.5 ? 4 * x * x * x : 1 - Math.pow(-2 * x + 2, 3) / 2;
	}
	function offsetTop(el) {
	    var rect = el.getBoundingClientRect(),
	    scrollTop = window.pageYOffset || document.documentElement.scrollTop;
	    return rect.top + scrollTop;
	}


	let header = qs('.header'),
		hamb = qs('.header__hamb'),
		menu = qs('.header__nav'),
		close = qs('.header__close');

	hamb.onclick = () => {
		menu.classList.add('active');
	}
	close.onclick = () => {
		menu.classList.remove('active');
	}

	window.onload = () => {
		let apesAll = qs('.hero__apes');
		let apes = qsa('.hero__apes img');
		
		apesAll.classList.add('active');

		setTimeout(() => {
			apes.forEach((ape) => {
				ape.classList.add('active')
				ape.dataset.visible = 'true'
			});
			apesAll.classList.remove('active');

			initHeroApes()
		}, 1000)

		function initHeroApes(){
			setInterval(() => {
				let activeApes = [...apes].filter(ape=>ape.classList.contains('active'));

				let currentInd = random(0, activeApes.length - 1);
				let currentInd2 = random(0, activeApes.length - 1);

				let current = activeApes[currentInd];
				let current2 = activeApes[currentInd2];

				current.classList.remove('active');

				setTimeout(() => {
					current.classList.add('active');
				}, 1500)
			}, 1500);
		}

		let titles = qsa('.slider__text'),
			titlesX = [0,0],
			titlesDir = ['', '-'],
			roadmapApes = qsa('.roadmap__img'),
			animated = [...roadmapApes, ...titles],
			contW = qs('.slider__texts').clientWidth;

		let options = { threshold: 0 };
		
		let callback = function(entries, observer) {
		    entries.forEach(entry => {
				entry.target.dataset.in_view = (entry.isIntersecting) ? 'true' : '';
	      	});
		};
		
		let observer = new IntersectionObserver(callback, options);
		animated.forEach(el => {
			observer.observe(el);
		});

		const raf = window.requestAnimationFrame ||
		    window.webkitRequestAnimationFrame ||
		    window.mozRequestAnimationFrame ||
		    window.oRequestAnimationFrame ||
		    window.msRequestAnimationFrame;

		var about = qs('.about'),
			scrolling = document.scrollingElement,
			divider = qs('.divider'),
			footer = qs('.footer'),
			sticky = qs('.hero__info.sticky'),
			smallDevice = window.innerWidth < 768;
			conditionTarget = smallDevice ? footer : about; 
			conditionValue = smallDevice ? window.innerHeight : 0;

		(function init() {
		    (function step() {
		    	if(conditionTarget.getBoundingClientRect().top <= conditionValue) {
		    		sticky.classList.add('active');
		    	} else {
		    		sticky.classList.remove('active');
		    	}

		    	if(scrolling.scrollTop > 0) {
		    		divider.classList.add('active');
		    	} else {
		    		divider.classList.remove('active');
		    	}

		    	if(titles){
		    		titles.forEach((el, ind) => {
			    		if(el.dataset.in_view == 'true') {
			    			let rect = el.getBoundingClientRect()
			    				winH = window.innerHeight;
			    			titlesX[ind] = (winH - rect.top) * (contW - el.clientWidth) / (winH + rect.height);

			    			el.style.transform = `translate3d(${titlesDir[ind]}${titlesX[ind]}px,0,0)`;
			    		}
			    	})
		    	} 

		    	if(roadmapApes) {
		    		roadmapApes.forEach((el, ind) => {
		    			if(el.dataset.in_view) {
		    				let rect = el.getBoundingClientRect(),
		    					stepH = el.parentElement.clientHeight;
		    				let y = (window.innerHeight - rect.top) * (stepH - rect.height) / (window.innerHeight + stepH);
		    				
		    				el.style.transform = `translate3d(0,${y}px,0)`;
		    			}
		    		})
		    	}
		    	

	           	raf(step);
		    })();
		})();
	}

	function random(min, max) {
	  min = Math.ceil(min);
	  max = Math.floor(max);
	  return Math.floor(Math.random() * (max - min + 1) + min);
	}


	if(!isTouchDevice) {
		var pointer = qs(".hero__circle div"),
			pointerBox = pointer.getBoundingClientRect(),
			centerPoint = window.getComputedStyle(pointer).transformOrigin,
			centers = centerPoint.split(" "),
			centerY = pointerBox.top + parseInt(centers[1]) - window.pageYOffset,
			centerX = pointerBox.left + parseInt(centers[0]) - window.pageXOffset;

		window.onmousemove = (e) => {
			let x = e.pageX,
				y = e.pageY,
				radians = Math.atan2(x - centerX, y - centerY),
				degree = (radians * (180 / Math.PI) * -1) + 180; 
		
			pointer.style.transform = "rotate("+degree+"deg)";
		}
	}
	

	var glide = new Glide('.glide', {
	  type: 'carousel',
	  animationDuration: 1200,
	  perView: 3,
	  autoplay: 5000,
	  swipeThreshold: 30,
	  hoverpause: false,
	  focusAt: 'center',
	  gap: 70,
	  breakpoints: {
	    767: {
	      gap: 12,
	      touchAngle: 60
	    },
	    500: {
	      perView: 2,
	      gap: 12,
	      touchAngle: 60
	    }
	  }
	})
	glide.on(['run'], function() {
	  qsa('.glide__slide').forEach((el,ind) => {
	  	el.classList.remove('shifted');

	  	if(glide.index % 2 && ind % 2 == 0) {
  			el.classList.add('shifted');
	  	} else if (glide.index % 2 == 0 && ind % 2) {
	  		el.classList.add('shifted');
	  	}
	  });
	})
	glide.mount();

	glide.go('=0');


	let questions = qsa('.faq__question');

	questions.forEach(el => {
		el.onclick = (e) => {
			let answer = el.nextElementSibling;
			if(el.classList.contains('active')) {
				el.classList.remove('active');
				answer.style.height = '0px';
			} else {
				el.classList.add('active');
				answer.style.height = answer.scrollHeight + 'px';
			}
		};
	})


	function qs (selector, searchIn) {
		return searchIn ? searchIn.querySelector(selector) : document.querySelector(selector)
	}
	function qsa (selector, searchIn) {
		return searchIn ? searchIn.querySelectorAll(selector) : document.querySelectorAll(selector)
	}

});