$(document).ready(function() {
  $(window).on("load resize", function() {
    if ($(this).outerWidth() < 767) {
      $(".menu").removeClass("menu-large").addClass("menu-small").delay(100).queue(function() {
        $(this).addClass("animated").dequeue();
      });
    } else {
      $(".menu").removeClass("menu-small").removeClass("animated").addClass("menu-large");
      $("#wrapper").removeClass("wrapper--active");
      $(".hamburger").removeClass("is-active");
      $(".menu").removeClass("menu--active");
    };
  });

	//menu
	$(".hamburger").on("click", function() {
		if ($(this).hasClass("is-active")) {
			$(".hamburger").removeClass("is-active");
			$(".menu").removeClass("menu--active");
			$("#wrapper").removeClass("wrapper--active");
		} else {
			$(".hamburger").addClass("is-active");
			$(".menu").addClass("menu--active");
			$("#wrapper").addClass("wrapper--active");
		};
	});
	//scroll spy
	$(window).on("scroll load", function() {
    var sT = $(window).scrollTop();
    var eOne = $("#home").offset().top;
    var eTwo = $("#about").offset().top;
    var eThree = $("#skills").offset().top;
    var eFour = $("#contact").offset().top;
    if (sT <= eTwo-100) {
      $(".home").addClass("menu__item--active");
      $(".home").siblings().removeClass("menu__item--active");
    } else if (sT <= eThree - 101) {
      $(".about").addClass("menu__item--active");
      $(".about").siblings().removeClass("menu__item--active");
    } else if (sT <= eFour - 100) {
      $(".skills").addClass("menu__item--active");
      $(".skills").siblings().removeClass("menu__item--active");
    } else if (sT >= eFour) {
    	$(".contact").addClass("menu__item--active");
      $(".contact").siblings().removeClass("menu__item--active");
    };
  });
  //smooth scroll
  $('.menu a').click(function() {
    if (location.pathname.replace(/^\//,'') == this.pathname.replace(/^\//,'') && location.hostname == this.hostname) {
      var target = $(this.hash);
      target = target.length ? target : $('[name=' + this.hash.slice(1) +']');
      if (target.length) {
        $('html, body').animate({
          scrollTop: target.offset().top
        }, 500);
        return false;
      }
    }
  });
  $('.btn-down').on('click', function(e) {
    e.preventDefault();
    $('html, body').animate({ scrollTop: $($(this).attr('href')).offset().top}, 500);
  });

  // background move
  $(window).on("load resize", function() {
    var width = $(window).outerWidth();
     var movementStrength;
    if (width < 993) {
      $("#home").css({
        "background-size": "cover",
        "background-position": "50%"
      });
      $("#home").mousemove(function(){
        $('#home').css("background-position", "50%");
      });
    } else {
      var movementStrength = 50;
      var height = movementStrength / $(window).height();
      var width = movementStrength / $(window).width();
      $("#home").css({
        "background-size": "calc(100vw + 60px)",
        // "background-position": "-50px"
      });
      $("#home").mousemove(function(e){
         var pageX = e.pageX - ($(window).width() / 2);
         var pageY = e.pageY - ($(window).height() / 2);
         var newvalueX = width * pageX * -1 - 50;
         var newvalueY = height * pageY * -1 - 50;
         $('#home').css("background-position", newvalueX+"px          " + newvalueY + "px");
      });
    };
  });
  

  //Progress bar

$(window).on("scroll load", function() {
    var sT = $(window).scrollTop();
    var element = $(".progress__bar1 div").offset().top;
    if (sT >= element - $(window).outerWidth()/2) {
      var width1 = $(".progress__bar1 div").attr("data-width");
      $(".progress__bar1 div").animate({
        width: width1 + "%"
      },1000);
      var width2 = $(".progress__bar2 div").attr("data-width");
      $(".progress__bar2 div").animate({
        width: width2 + "%"
      },1500);
      var width3 = $(".progress__bar3 div").attr("data-width");
      $(".progress__bar3 div").animate({
        width: width3 + "%"
      },2000);
    };
});
  

  //Scroll reveal
  window.sr = ScrollReveal();
  sr.reveal("h1", {
  duration: 1000,
  origin: "top",
  });
  sr.reveal("h1 + p", {
  duration: 1000,
  origin: "bottom",
  delay: 500
  });
  sr.reveal(".btn-down", {
  duration: 1000,
  origin: "bottom",
  delay: 1000
  });
  sr.reveal(".block__header", {
  duration: 1000,
  origin: "bottom",
  viewFactor: 0.2,
  delay: 200
  });
  sr.reveal(".about__left", {
  duration: 1000,
  origin: "left",
  viewFactor: 0.2,
  delay: 500,
  distance: "100px"
  });
  sr.reveal(".about__right", {
  duration: 1000,
  origin: "right",
  viewFactor: 0.2,
  delay: 500,
  distance: "100px"
  });
  sr.reveal(".form", {
  duration: 1000,
  origin: "bottom",
  viewFactor: 0.2,
  delay: 500
  });

  //Плавный скролл
  // Custom Easing
  
  
});