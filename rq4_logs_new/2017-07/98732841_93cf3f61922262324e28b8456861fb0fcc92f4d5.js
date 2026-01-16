!function(a){a(function(){a(".button-collapse").sideNav({closeOnClick: true, draggable: true}),a(".parallax").parallax(),a("header .search").click(function(){a("#search").css("display","block"),a("#search .scale-transition").addClass("scale-in")}),a("section#search .close").click(function(){a("#search").css("display","none"),a("#search .scale-transition").addClass("scale-out")}),function(){var a;if(a=window.driftt=window.drift=window.driftt||[],!a.init)a.invoked?window.console&&console.error&&console.error("Drift snippet included twice."):(a.invoked=!0,a.methods=["identify","track","reset","debug","show","ping","page","hide","off","on"],a.factory=function(b){return function(){var c;return c=Array.prototype.slice.call(arguments),c.unshift(b),a.push(c),a}},a.methods.forEach(function(b){a[b]=a.factory(b)}),a.load=function(a){var b,c,d,e;b=3e5,e=Math.ceil(new Date/b)*b,d=document.createElement("script"),d.type="text/javascript",d.async=!0,d.crossorigin="anonymous",d.src="https://js.driftt.com/include/"+e+"/"+a+".js",c=document.getElementsByTagName("script")[0],c.parentNode.insertBefore(d,c)})}(),drift.SNIPPET_VERSION="0.2.0",drift.load("r9849b4dtwz3"),a(".carousel.carousel-slider").carousel({fullWidth:!0})})}(jQuery);

$(document).ready(function(){

  $('img').one('error', function() {
    dataLayer.push({
      'event': 'brokenImage',
      'eventLabel': this.src
    });
    // this.src = 'http://yoldanciktim.com/wp-content/themes/YoldanCiktim/assets/ui/broken-img.jpg';
    $(this).hide();
  });

  $('.collapsible').collapsible();

  // $('figure img, .entry img').materialbox();

  var options = [
    {selector: '.innerBox form', offset: 200, callback: function() {
      $('.innerBox form').css('border-color', '#000');
    }},
  ];
  Materialize.scrollFire(options);

});