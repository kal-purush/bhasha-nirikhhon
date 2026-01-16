//3秒滚动
var setIntervalID;
var intervalTime = 6000;  // 间隔时间
var nextID = 1;  // 下张图片ID
var activeID = 0; //当前图片
var imgCount = 2;
var contextPath = './';

$(function(){

	$("#header-menu #index").addClass("selected");
	
	//向左滚动（前滚动）
	$("#previousImage").mouseover(function(){
		$(this).attr("src",contextPath + "image/previousIconHover.png");
	}).mouseout(function(){
		$(this).attr("src",contextPath + "image/previousIcon.png");
	}).mouseup(function(){
		//关闭
		clearInterval(setIntervalID);
		
		nextID = parseInt(activeID) - 1 >=0 ? parseInt(activeID) - 1 : 0;

		rotate(nextID);
		
	});
	
	//向右滚动（后滚动）
	$("#nextImage").mouseover(function(){
		$(this).attr("src",contextPath + "image/nextIconHover.png");
	}).mouseout(function(){
		$(this).attr("src",contextPath + "image/nextIcon.png");
	}).mouseup(function(){
		//关闭
		clearInterval(setIntervalID);
		
		nextID = (parseInt(activeID) + 1) % imgCount; 
		
		rotate(nextID);
		
	});
	
	//小圆点点击
	$("li[name='dotImage']").mouseover(function(){
		if($(this).attr("title") == activeID) {
			$(this).attr("src",contextPath + "image/dotBigHover.png");	
			//clearInterval(setIntervalID);
		} else {
			$(this).attr("src",contextPath + "image/dotSmallHover.png");	
		}
	}).mouseout(function(){
		if($(this).attr("title") == activeID) {
			$(this).attr("src",contextPath + "image/dotBigNormal.png");	
			//开启
			//timeout();
		} else {
			$(this).attr("src",contextPath + "image/dotSmallNormal.png");	
		}
	}).mouseup(function(){
		if($(this).attr("title") == activeID) {	
			return;
		}
		
		clearInterval(setIntervalID);
		var clickID = parseInt($(this).attr("title"));
		rotate(clickID);	
	});
	
	
	timeout();
	
});

/**
 * 针对IOS打开网页
 * @param url
 */
function openUrl(url){
    var f=document.createElement("form");
    f.setAttribute("action" , url );
    f.setAttribute("method" , 'post' );
    f.setAttribute("target" , '_blank' );
    document.body.appendChild(f);
    f.submit();
 }

/**
 * 图片每隔4秒变化
 */
function timeout(){
	setIntervalID=setInterval(rotate,intervalTime);
}

//图片轮换函数
var rotate=function(clickID){
	if(clickID){ 
		nextID = clickID; 
		clearInterval(setIntervalID);
	} else { 
		nextID = (parseInt(activeID) + 1) % imgCount; 
	}
	
	//$("#dotImage" + activeID).attr("src",contextPath + "image/dotSmallNormal.png");	
	//$("#dotImage" + nextID).attr("src",contextPath + "image/dotBigNormal.png");
	
	//alert(activeID + ' '+ nextID);
	$("#dotImage" + activeID).removeClass("active");
	$("#dotImage" + nextID).addClass("active");
	
	$("#index-content-second-" + activeID).fadeOut(1200);
	$("#index-content-second-" + nextID).fadeIn(1200);
	
	activeID = nextID;
	
	if(clickID) {
		timeout();
	}
};