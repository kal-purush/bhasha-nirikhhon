var hamburgerToggle = $('#toggle-nav');
var sideNav = $ ('.side-nav');
var sideNavHome = $('.sn-home');
var sideNavWork = $('.sn-work');
var sideNavAbout = $('.sn-about');
var sideNavContact = $('.sn-contact');

var sideNavClickHandler = function (e) {
	var isActive = sideNav.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		sideNav.attr('data-state','inactive');
	} else {
		sideNav.attr('data-state','active');
	}
};

hamburgerToggle.on('click', sideNavClickHandler);

var sideNavItemClick = function () {
	var isActive = sideNav.attr('data-state');

	if (isActive == 'active') {
		sideNav.attr('data-state','inactive');
	} else {
		sideNav.attr('data-state','active');
	}
};

sideNavHome.on('click', sideNavItemClick);
sideNavWork.on('click', sideNavItemClick);
sideNavAbout.on('click', sideNavItemClick);
sideNavContact.on('click', sideNavItemClick);


// Hamburger Toggle Animation

$( "#toggle-nav" ).on( "click", function() {
  $( this ).toggleClass( "active" );
  $( this ).blur();
});

$( ".sn-home" ).on( "click", function() {
  $( "#toggle-nav" ).toggleClass( "active" );
});

$( ".sn-work" ).on( "click", function() {
  $( "#toggle-nav" ).toggleClass( "active" );
});

$( ".sn-about" ).on( "click", function() {
  $( "#toggle-nav" ).toggleClass( "active" );
});

$( ".sn-contact" ).on( "click", function() {
  $( "#toggle-nav" ).toggleClass( "active" );
});




var hashTagActive = "";
$(document).ready( function() {
	$(".smooth-scroll li a").click(function (event) {

		if(hashTagActive != this.hash) { //this will prevent if the user click several times the same link to freeze the scroll.
			event.preventDefault();

			//calculate destination place
			var dest = 0;
			if ( $(this.hash).offset().top > $(document).height() - $(window).height() ) {
				dest = $(document).height() - $(window).height();
			}
			else {
				dest = $(this.hash).offset().top;
			}

			//go to destination
			$('html, body').animate({
			    scrollTop: dest
			}, 700, 'swing');

			hashTagActive = this.hash;
		}

	});
});


// Masonry

var container = document.querySelector('#masonry');
var masonry = new Masonry(container, {
    columnWidth: 0,
    itemSelector: '.item'
});


var $workSection = $ ('.work-section');
var $topNav = $ ('.top-nav');
var $landingSection = $ ('.landing-section');
var $aboutSection = $('.about-section');
var $aboutWrapper = $ ('.about__wrapper');


if (window.screen.width >= 768) {
	$workSection.waypoint(function (direction) {
		if (direction == 'down') {
			$topNav.addClass('sticky-nav');
		} else {
			$topNav.removeClass('sticky-nav');
		}
	}, { offset: '150px'});
}

$aboutSection.waypoint(function () {
	$aboutWrapper.addClass('about__show');
}, {offset: '200px'});


// Item Light Box
$(".work-item-container").on("click",function (e){
	e.stopImmediatePropagation();
});


var itemClickOne = $ ('.item-click-1');
var itemClickTwo = $ ('.item-click-2');
var itemClickThree = $ ('.item-click-3');
var itemClickFour = $ ('.item-click-4');
var itemClickFive = $ ('.item-click-5');
var itemClickSix = $ ('.item-click-6');
var itemClickSeven = $ ('.item-click-7');
var workItemClickOne = $('.work-item-click-1');
var workItemClickTwo = $('.work-item-click-2');
var workItemClickThree = $('.work-item-click-3');
var workItemClickFour = $('.work-item-click-4');
var workItemClickFive = $('.work-item-click-5');
var workItemClickSix = $('.work-item-click-6');
var workItemClickSeven = $('.work-item-click-7');
var bodyContent = $ ('.body-content');
var workItem = $ ('.work-item');

var workItemClickHandler = function (e) {
	var isActive = workItemClickOne.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickOne.attr('data-state','inactive');
	} else {
		workItemClickOne.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickOne.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickOne.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickOne.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickOne.attr('data-state','active');
	}
};

workItemClickOne.on('click', workItemClose);

// Item 2

var workItemClickHandler = function (e) {
	var isActive = workItemClickTwo.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickTwo.attr('data-state','inactive');
	} else {
		workItemClickTwo.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickTwo.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickTwo.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickTwo.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickTwo.attr('data-state','active');
	}
};

workItemClickTwo.on('click', workItemClose);

// Item 3

var workItemClickHandler = function (e) {
	var isActive = workItemClickThree.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickThree.attr('data-state','inactive');
	} else {
		workItemClickThree.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickThree.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickThree.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickThree.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickThree.attr('data-state','active');
	}
};

workItemClickThree.on('click', workItemClose);

// Item 4

var workItemClickHandler = function (e) {
	var isActive = workItemClickFour.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickFour.attr('data-state','inactive');
	} else {
		workItemClickFour.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickFour.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickFour.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickFour.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickFour.attr('data-state','active');
	}
};

workItemClickFour.on('click', workItemClose);


// Item 5

var workItemClickHandler = function (e) {
	var isActive = workItemClickFive.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickFive.attr('data-state','inactive');
	} else {
		workItemClickFive.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickFive.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickFive.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickFive.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickFive.attr('data-state','active');
	}
};

workItemClickFive.on('click', workItemClose);

// Item 6

var workItemClickHandler = function (e) {
	var isActive = workItemClickSix.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickSix.attr('data-state','inactive');
	} else {
		workItemClickSix.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickSix.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickSix.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickSix.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickSix.attr('data-state','active');
	}
};

workItemClickSix.on('click', workItemClose);

// Item 7

var workItemClickHandler = function (e) {
	var isActive = workItemClickSeven.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickSeven.attr('data-state','inactive');
	} else {
		workItemClickSeven.attr('data-state','active');
		bodyContent.addClass('noscroll');
	}
};

itemClickSeven.on('click', workItemClickHandler);

var workItemClose = function (e) {
	var isActive = workItemClickSeven.attr('data-state');
	e.preventDefault ();

	if (isActive == 'active') {
		workItemClickSeven.attr('data-state','inactive');
		bodyContent.removeClass('noscroll');
	} else {
		workItemClickSeven.attr('data-state','active');
	}
};

workItemClickSeven.on('click', workItemClose);

	 $(document).keyup(function(e) {
        if (e.keyCode == 27) { // esc keycode
            workItem.attr('data-state', 'inactive');
            bodyContent.removeClass('noscroll');
        }
    });