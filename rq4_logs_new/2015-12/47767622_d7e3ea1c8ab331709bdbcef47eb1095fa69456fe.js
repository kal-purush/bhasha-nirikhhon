$(function(){
	var originW=640,
	originH=1136,
	baseFontSize=40;/*等比缩放的文字大小1rem就是当前宽度除以设计图宽度*40得到的*/
	var baseDom=$(".swiper-wrapper");
	centerPage();
	var debounceTimer=null;
	$(window).on('resize',function(){
		clearTimeout(debounceTimer);
		debounceTimer=setTimeout(centerPage,40);
	});
	/*当屏幕宽度大于高度时,保持高度100%,
	当屏幕高度大于宽度时,保持宽度100%
	而另外一个的尺寸则按照设计图的尺寸(640*1026)与当前100%的值进行等比例缩放,
	同时变化的还有html的font-size,html的fontsize是基于scaleWidth的*/
	function centerPage(){
		var originRadio=originW/originH;
		var absHeight=$(baseDom).height();
		var absWidth=$(baseDom).width();
		var absRadio=absWidth/absHeight;
		//计算重置样式,首先保证高度100%,宽度按比例缩放
		var resizeH=absHeight;
		var resizeW=originW*absHeight/originH;
		if(resizeW>=absWidth){//当意识到宽度大于屏幕宽度时,保证宽度100%,高度按比例缩放
			resizeW=absWidth;
			resizeH=originH*absWidth/originW;
		}
		$(".page-inner").css({
			'width':resizeW,
			'height':resizeH,
			'margin-left':-resizeW/2,
			'margin-top':-resizeH/2
		});
		$('html').css('font-size',[baseFontSize*resizeW/originW,'px'].join(''));
	}
});