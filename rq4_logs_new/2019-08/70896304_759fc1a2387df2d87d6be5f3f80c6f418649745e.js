/**
 *获取scrollTop的值，兼容所有浏览器 
 */
function getScrollTop() {
	var scrollTop = document.documentElement.scrollTop || window.pageYOffset || document.body.scrollTop;
	return scrollTop;
}

/**
 *设置scrollTop的值，兼容所有浏览器 
 */
function setScrollTop(scroll_top) {
	document.documentElement.scrollTop = scroll_top;
	window.pageYOffset = scroll_top;
	document.body.scrollTop = scroll_top;
}