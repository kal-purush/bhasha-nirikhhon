
// VARS
    
    var $window = $(window);
    var windowSize = $window.width();
    var windowHeight = $window.height();
    var portfolioGrid = $('#portfolio-grid-container');
    var menuStyle = $('nav').attr('id');      
       
    
    /*----------------------------------------------------*/
    /* MOBILE DETECT FUNCTIONS
    /*----------------------------------------------------*/

    var isMobile = {
        Android: function() {
            return navigator.userAgent.match(/Android/i);

        },
        BlackBerry: function() {
            return navigator.userAgent.match(/BlackBerry/i);
        },
        iOS: function() {
            return navigator.userAgent.match(/iPhone|iPad|iPod/i);
        },
        Opera: function() {
            return navigator.userAgent.match(/Opera Mini/i);
        },
        Windows: function() {
            return navigator.userAgent.match(/IEMobile/i);
        },
        any: function() {
            return (isMobile.Android() || isMobile.BlackBerry() || isMobile.iOS() || isMobile.Opera() || isMobile.Windows());
        }
    };	


 $window.load(function() {
     
     /* --------------------------------*/
    /* - onscroll Effect script start
    /* -------------------------------*/
     
     function onScroll(event){
            var scrollPos = $(document).scrollTop();
            $('#verticalNav a').each(function () {
                var currLink = $(this);
                var refElement = $(currLink.attr("href"));
                if (refElement.position().top <= scrollPos && refElement.position().top + refElement.height() > scrollPos) {
                   $('#verticalNav ul li a').removeClass("active");
                    currLink.addClass("active");
                        if(currLink.hasClass('active')){
                           currLink.find("span").show();
                        }
                }
                else{
                        currLink.find("span").hide();
                }
            });
        }
     /* --------------------------------*/
    /* - onscroll Effect script start
    /* -------------------------------*/
     
     

    /* --------------------------------*/
    /* - Parallax Effect on other than mobile
    /* -------------------------------*/
        if (!isMobile.any()) {
            
            $('.parallax').each(function() {
                $(this).parallax("50%", 0.2);
            });
        }


});

/* --------------------------------*/
    /* - Window Scroll
     /* -------------------------------*/
    $window.scroll(function() {     

        if (menuStyle == 'nav-top') {
            if ($(".navbar").offset().top > 200) {
                $(".navbar-fixed-top").addClass("top-nav-collapse");
                $(".navbar-fixed-top").removeClass("default-nav");
            } else {
                $(".navbar-fixed-top").removeClass("top-nav-collapse");
            }
        }
        

        if (menuStyle != 'nav-top' && !isMobile.any()) {
            onepageScroller();
        }

    }); // End on window scroll


 /* --------------------------------*/
 /* - Document Ready
 /* -------------------------------*/

    $(document).ready(function() {
        $("a[href='#pageWrapper']").click(function() {
              $("html, body").animate({ scrollTop: 0 }, 1500);
              return false;
            });

        /*$("#loader").delay(2500).fadeOut();
        $(".mask").delay(3000).fadeOut("slow");
*/
    	$('.pageWrapper, #owl-home, .cd-section').css({'height': windowHeight + 'px'});   

    	//BG Image
        $("section, div, figure").each(function(indx) {
            if ($(this).attr("data-background")) {
                $(this).css("background-image", "url(" + $(this).data("background") + ")");
            }
        });

        //Parallax
        $.fn.parallax = function(xpos, speedFactor, outerHeight) {
        var $this = $(this);
        var getHeight;
        var firstTop;
        var paddingTop = 0;
        //get the starting position of each element to have parallax applied to it	
        function update() {

            $this.each(function() {

                firstTop = $this.offset().top;
            });
            if (outerHeight) {
                getHeight = function(jqo) {
                    return jqo.outerHeight(true);
                };
            } else {
                getHeight = function(jqo) {
                    return jqo.height();
                };
            }

            // setup defaults if arguments aren't specified
            if (arguments.length < 1 || xpos === null)
                xpos = "50%";
            if (arguments.length < 2 || speedFactor === null)
                speedFactor = 0.5;
            if (arguments.length < 3 || outerHeight === null)
                outerHeight = true;
            // function to be called whenever the window is scrolled or resized

            var pos = $window.scrollTop();
            $this.each(function() {
                var $element = $(this);
                var top = $element.offset().top;
                var height = getHeight($element);
                // Check if totally above or totally below viewport
                if (top + height < pos || top > pos + windowHeight) {
                    return;
                }

                $this.css('backgroundPosition', xpos + " " + Math.round((firstTop - pos) * speedFactor) + "px");
            });
        }

        $window.on('scroll', update).resize(update);
        update();
    };

 
    /*----------------------------------------------------*/
    /*      init cubeportfolio
    /*----------------------------------------------------*/

    portfolioGrid.cubeportfolio({
        filters: '#filters-container',
        loadMore: '#loadMore-container',
        loadMoreAction: 'click',
        layoutMode: 'grid',
        rewindNav: true,
        scrollByPage: false,
        defaultFilter: '*',
        animationType: portfolioGrid.data('animationtype'),
        gapHorizontal: portfolioGrid.data('gaphorizontal'),
        gapVertical: portfolioGrid.data('gapvertical'),
        gridAdjustment: 'responsive',
        mediaQueries: [{
                width: 1100,
                cols: 3
            }, {
                width: 800,
                cols: 2
            }, {
                width: 500,
                cols: 2
            }, {
                width: 320,
                cols: 1
            }],
        caption: portfolioGrid.data('caption'),
        displayType: 'lazyLoading',
        displayTypeSpeed: 100,
        // lightbox
        lightboxDelegate: '.cbp-lightbox',
        lightboxGallery: true,
        lightboxTitleSrc: 'data-title',
        lightboxCounter: '<div class="cbp-popup-lightbox-counter">{{current}} of {{total}}</div>',
        // singlePage popup
        singlePageDelegate: '.cbp-singlePage',
        singlePageDeeplinking: true,
        singlePageStickyNavigation: true,
        singlePageCounter: '<div class="cbp-popup-singlePage-counter">{{current}} of {{total}}</div>',
        
    });


	/*----------------------------------------------------*/
    /*      Vartical Nav
    /*----------------------------------------------------*/
	$(document).on("scroll", onScroll);

	var arrayVariable = [ ];
	var sectionName = [ ];
	$(".cd-section").each(function (i)
	{
	    arrayVariable[i]=$(this).attr("id")
	    sectionName[i]=$(this).attr("data-name")
	    console.log()
	});

	console.log(arrayVariable)
	for (var i=0; i < arrayVariable.length; i++){
	   $("#verticalNav ul").append('<li>' + '<a href=' + '#' + arrayVariable[i] + '>' + '<span>'+ sectionName[i] + '</span>'   + '</li>')
	}

	$('#verticalNav li:first-child a').addClass('active');

	$('#verticalNav li a').bind('click',function(event){
	  var $anchor = $(this);
	  $('html, body').stop().animate({        
	  scrollTop: $($anchor.attr('href')).offset().top - 0
	  }, 1500,'easeInOutExpo');
	  event.preventDefault();
	   
	});
   
/****************************************** Changes by Satish *********************************************************/        
    /*$('#verticalNav li a').bind('click',function(event){
      var $anchor = $(this);
      $('html, body').stop().animate({scrollTop: $($anchor.attr('href')).offset().top - 0}, 0,'easeInOutExpo');
      event.preventDefault();

    });*/
/****************************************** Changes by Satish *********************************************************/        

	function onScroll(event){
	    var scrollPos = $(document).scrollTop();
	    $('#verticalNav a').each(function () {
	        var currLink = $(this);
	        var refElement = $(currLink.attr("href"));
	        if (refElement.position().top <= scrollPos && refElement.position().top + refElement.height() > scrollPos) {
	            $('#verticalNav ul li a').removeClass("active");
	            currLink.addClass("active");
	        }
	        else{
	            currLink.removeClass("active");
	        }
	    });
	}

	//Div height adjustment
    var contactDiv = $('.left').outerHeight();
    console.log(contactDiv)

   //Google Map setting
   $('.googleMapWrapper, iframe').css({'height': contactDiv / 2 + 'px'});
   $('.address').css({'height': contactDiv / 2 + 'px'});


   $('.navbar-toggle').on('click', function() {
            $(this).toggleClass('active');
        });

        if (menuStyle == 'nav-top') {
            
            var menuheight = (isMobile.any() || windowSize < 1299) ? 0 : 0;
            $('.nav li a, .navbar-brand, .move-down').on('click', function(event) {
                var $anchor = $(this);
                $('html, body').stop().animate({
                    scrollTop: $($anchor.attr('href')).offset().top - menuheight
                }, 1500, 'easeInOutExpo');
                event.preventDefault();
            });

            /* ==============================================
             CLOSE COLLAPSE NAV ON MOBILE DEVICES
             =============================================== */
            if (isMobile.any() || windowSize < 1299) {
                
                $('.nav li a, .navbar-brand').on('click', function() {
                    $(".navbar-collapse").collapse('hide');
                    $('.navbar-toggle').removeClass('active');
                });
            }
        }

        function onepageScroller() {       
        
        $('nav').find('li a').removeClass('active');
        $('nav').find('a[href="#' + onePageCurrentSection() + '"]').addClass('active');
    }

    });