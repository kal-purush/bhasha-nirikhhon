// create or use existing site scope
var Site = Site || {};

/**
 *	Animate object for animating elements triggered by position of window 
 *
 *
 */

function animate(container,container_elements,trigger_element,delay_time) {
 	var self = this;

 	self.container = container;
 	self.container_elements = null;
 	self.position = trigger_element.offset().top -300;
 	self.delay_time = delay_time;
 	self.active = false;

	/*
	 * object initialization 
	 */
	 self._init = function() {
        // connect signals
        $(window).on('scroll', self.handle_scroll);

        // Find all container elements
        self.container_elements = self.container.find(container_elements);

	 }

	 self.handle_scroll = function(event) {
	 	var over_position = $(window).scrollTop() >= self.position;

        if (over_position && !self.active) {

			self.container_elements.each(function(index) {
		        var item = self.container_elements.eq(index);
			        if(!item.hasClass('active')){
			        setTimeout(function() {
		                self.handle_active(item);
			        }, self.delay_time + (index * self.delay_time));
			    }
			});

            self.active = true;

        } else if (!over_position && self.active) {
            clearTimeout();
            self.active = false;
        }

	 }

	 self.handle_active = function(item) {
	 	console.log(item);
	 	item.addClass('active');
	 }

	// finish object initialization
	self._init();	 
 }

$(function() {
	Site.animate = new animate($('ul#timeline'),$('ul#timeline li'),$('section.header'),800)
})