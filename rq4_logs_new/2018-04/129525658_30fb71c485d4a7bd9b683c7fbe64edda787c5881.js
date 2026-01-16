/*
******
	***	Anchor Slider by Cedric Dugas ***
  *** Version 2.0.0 ***
	*** Http://www.position-absolute.com ***
	Never have an anchor jumping your content, slide it.
	Don't forget to put an id to your anchor !
	You can use and modify this script for any project you want, but please leave this comment as credit.
****
*/

(function ($) {
  $.fn.anchorAnimate = function(options) {
    var settings = $.extend({}, $.fn.anchorAnimate.defaults, options);

    return this.each(function() {
      var caller = this;
      $(caller).click(function(event) {
        event.preventDefault();
        var elementClick = $(caller).attr("href");

        var destination = $(elementClick).offset().top + settings.offset;
        $("html:not(:animated), body:not(:animated)").animate({ scrollTop: destination}, settings.speed, function() {
          if(0 === settings.offset) {
            window.location.hash = elementClick;
          }
        });
        return false;
      });
    });
  };

  $.fn.anchorAnimate.defaults = {
    speed: 800,
    offset: 0
  };
}(jQuery));
// zarasa

jQuery.fn.extend({
    toggleText: function (a, b){
        var that = this;
            if (that.text() != a && that.text() != b){
                that.text(a);
            }
            else
            if (that.text() == a){
                that.text(b);
            }
            else
            if (that.text() == b){
                that.text(a);
            }
        return this;
    }
});


//Change tab function
function change_tab(id){
   // Show/Hide Tabs
   $(id).show().siblings().hide(); 
   $('a[href="'+id+'"]',".tab-links").parent('li').addClass('active').siblings().removeClass('active');
   // Change/remove current tab to active 
}





$.fn.anchorAnimate.defaults.speed = 800;
$.fn.anchorAnimate.defaults.offset = -80;

$(document).ready(function() {
	$("a.anchorLink").anchorAnimate();
	
	// Ver más
	$(".more").click(function (){
		$(".row3").slideToggle();
		$(".row3").toggleClass("anima");
		$(this).toggleText("Ver más", "Ocultar");
	});
	
	// Tabs
	$('.tabs .tab-links a').on('click', function(e) {
	   var currentAttrValue = $(this).attr('href'); 
	   change_tab(currentAttrValue);
       e.preventDefault();
    });
	
});
