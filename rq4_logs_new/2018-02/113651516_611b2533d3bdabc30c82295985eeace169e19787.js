
// 获取对象的Style属性
function getStyle(obj,style) {
    //currentStyle IE
    //getComputedStyle Chrome,FF,Safari
    // 可以获取元素的最终样式，包括浏览器的默认值，而不像style只能获取行间样式，所以更常用到。 注意：不能获取复合样式如background属性值，只能获取单一样式如background-color等
    return obj.currentStyle ? obj.currentStyle[style] : window.getComputedStyle(obj, null)[style];
}