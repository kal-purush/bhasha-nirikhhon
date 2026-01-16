(function(){
  'use strict';

  /* code here! */

  console.log('hello world!');


  

  /**
   * 设计要被激活的dom
   * @param {[type]} active [当前将要被激活的dom]
   */
  function setActive(active){
  	active.addClass('.carousel-slide-active');
  }

  /**
   * 删除被激活的样式
   * @param  {[type]} actived [当前被激活的dom]
   * @return {[type]}         [description]
   */
  function removeActive(actived){
  	actived.removeClass('.carousel-slide-active');
  }

  /**
   * 初始化的时候默认中间的图片为显示第一张
   * @return {[type]} [description]
   */
  function findMidIndex(){
  	return Math.ceil($(".carousel-slide").length / 2);
  }

  /**
   * 找到当前被激活的节点
   * @return {[type]} [description]
   */
  function findActive(){
  	return $(".carousel-slide-active");
  }

  /**
   * 找到被激活节点的前一个节点
   * @return {[type]} [description]
   */
  function findPrev(){
  	return $(".carousel-slide-active").prev();
  }

  /**
   * 找到当前节点的下一个节点
   * @return {[type]} [description]
   */
  function findNext(){
  	return $(".carousel-slide-active").next();
  }

  /**
   * 向前更换被激活的dom
   * @return {[type]} [description]
   */
  function changePreIndex(){
  	var pre = findPrev();
  	var active = findActive();
  	setActive(pre);
  	removeActive(active);
  }

  /**
   * 向后更换呗激活的dom
   * @return {[type]} [description]
   */
  function changeNextActive(){
  	var next = findNext();
  	var active = findActive();
  	setActive(next);
  	removeActive(active);
  }

  /**
   * 检查是否存在前一个元素
   * @return {[type]} [description]
   */
  function checkPrev(){
  	return $(".carousel-slide-active").prev() === [];
  }

  /**
   * 检查是否存在后一个元素
   * @return {[type]} [description]
   */
  function checkNext(){
  	return $(".carousel-slide-active").next() === [];
  }

  /**
   * 初始化draw
   * @return {[type]} [description]
   */
  function initDraw(){
  	var active = findActive();
  	var pre = findPrev();
  	var next = findNext();

  }

  /**
   * 初始化激活
   * @return {[type]} [description]
   */
  function initSetActive(){
  	var index = findMidIndex();
  	$(".carousel-slide:nth-child("+index+")").addClass('carousel-slide-active');
  }

  /**
   * 设置silde的宽度，和wrapper的宽度
   */
  function initSetWidth(){
  	//获取slide的最大宽度，一切按照最大宽度来计算
  	var maxWidth = 0;
  	for(var i = 0 ; i < $(".carousel-slide").length; i++){
  		maxWidth > $(".carousel-slide")[i].offsetWidth ? maxWidth : maxWidth = $(".carousel-slide")[i].offsetWidth;
  	}
  	//将所有元素赋值为最大宽度
  	$(".carousel-slide").width(maxWidth);
  	$(".carousel-slide img").attr("draggable","false");
  	//获取所有slide的宽度和
  	var widthSum = $(".carousel-slide").length * maxWidth;
  	$(".carousel-wrapper").width(widthSum);
  	console.log(maxWidth);
  	console.log(widthSum);
  }

  function init(){
  	initSetWidth();
  	initSetActive();
  	// initDraw();
  }

  init();

  var touchend = 'ontouchend' in window ? "touchend" : "mouseup";
  var touchstrat = 'ontouchstart' in window ? "touchstrat" : "mousedown";
  var touchmove = 'ontouchmove' in window ? "touchmove" : "mousemove";
  var startX,endX,moveX = 50;

  $(document).on(touchstrat,".carousel-slide",function(e){
  	startX = e.clientX;
  	console.log(e.clientX);
  });

  $(document).on(touchend,".carousel-slide",function(e){
  	endX = e.clientX;
  	startX = null;
  });

  $(document).on(touchmove,".carousel-slide",function(e){
  	if(!startX){
  		return;
  	}
  	moveX = e.clientX - startX;
  	
  });



})();