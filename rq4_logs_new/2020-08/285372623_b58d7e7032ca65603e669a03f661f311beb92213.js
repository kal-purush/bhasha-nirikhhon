$(document).ready(function(){
	/*
    // Add scrollspy to <body>
    $('body').scrollspy({target: ".navbar", offset: 0});   


    // Add smooth scrolling on all links inside the navbar
    $(".navigationSidebar__navList a").on('click', function(event) {

    // Make sure this.hash has a value before overriding default behavior
    if (this.hash !== "") {

        // Prevent default anchor click behavior
        event.preventDefault();

        // Store hash
        var hash = this.hash;

        // Using jQuery's animate() method to add smooth page scroll
        // The optional number (800) specifies the number of milliseconds it takes to scroll to the specified area
        $('html, body').animate({
        scrollTop: $(hash).offset().top
        }, 800, function(){

        // Add hash (#) to URL when done scrolling (default click behavior)
        window.location.hash = hash;
        });

    } // End if

    });

    $('#menu-toggle').click(function(){
        var $navbar = $('.navigationSidebar__nav');

        $(this).toggleClass('open');
        $navbar.toggleClass('open');
    });

    $('.navigationSidebar__navListItemLink').click(function(){
        var $navbar = $('.navigationSidebar__nav');
        var $menu = $('#menu-toggle');

        if ($menu.hasClass('open')) {
            $menu.toggleClass('open');
            $navbar.toggleClass('open');
        }
		});
		*/

    // When an element comes into viewport


    function inView() {
        $.fn.visible = function (partial) {
    
            var $t = $(this),
                $w = $(window),
                viewTop = $w.scrollTop(),
                viewBottom = viewTop + $w.height(),
                _top = $t.offset().top,
                _bottom = _top + $t.height(),
                compareTop = partial === true ? _bottom : _top,
                compareBottom = partial === true ? _top : _bottom;
    
            return ((compareBottom <= viewBottom) && (compareTop >= viewTop));
    
        };
    
        var win = $(window);
    
        var $animated = $(".animated");
    
        $animated.each(function (i, el) {
            var el = $(el);
            if (el.visible(true)) {
                el.addClass("already-visible");
            }
        });
    
        win.scroll(function (event) {
            $animated.each(function (i, el) {
                var el = $(el);
                if (el.visible(true)) {
                    el.addClass("come-in");
                }
            });
        });
    };
    inView();

});