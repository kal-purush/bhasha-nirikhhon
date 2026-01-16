(function($){
	$.fn.carousel = function(options){
		var $this = $(this);
		var defaults = {
			auto: true, //是否自动
			mouseover: false, //鼠标悬停，是否暂停轮播
			speed: 3000 //默认切换速度
		};
		
	};
})(jQuery);

$(function(){
  $('.carousel').carousel({
    speed:5000
  });
  $("li.stop,li.full").css({
    'cursor':'pointer'
  })
  $('li.stop,li.full').click(function(event) {
    window.location.href=$(this).data('url');
  });
  
  $('.tabs a').mouseover(function() {
    var $this = $(this);
    if (!$this.hasClass('active')){
      $this.addClass('active')
        .siblings().removeClass('active')
        .closest('.tab-item').find('.item').hide()
        .eq($this.index()).show();
    }
  });
  $('.carousel .right-frame dt a').mouseover(function(){
    var $this = $(this);
    if (!$this.hasClass('active')){
      $this.addClass('active')
        .siblings().removeClass('active')
        .closest('.right-frame').find('dd').hide()
        .eq($this.index()).show();
    }
  });
  
 
  (function(){
    var top = $('.comparison').offset().top
      wnd_height = $(window).height();
    
    if ($('.comparison .zjw').index() != -1){
      var $zjw = $('.comparison .zjw .progress-bar'),
        progress = $zjw.width();
      $zjw.css('width', '0');
      $(window).scroll(function(){
        var scroll_top = $(this).scrollTop();
        if (wnd_height + scroll_top > top + 200) {
          $zjw.animate({width: progress}, 1500);
        }
      });
    }
  })();

 
  
 
});