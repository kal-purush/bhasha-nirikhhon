/**
 * test transform.js
 */
var AlloyTouch = require ("alloytouch");
var Transform = require ("css3transform");
var element1 = document.querySelector("#test1");
Transform(element1);
new AlloyTouch({
    touch:"#test1",//反馈触摸的dom
    vertical: false,//不必需，默认是true代表监听竖直方向touch
    target: element1, //运动的对象
    property: "translateX",  //被运动的属性
    // min: 100, //不必需,运动属性的最小值
    // max: 2000, //不必需,滚动属性的最大值
    sensitivity: 1,//不必需,触摸区域的灵敏度，默认值为1，可以为负数
    factor: 1,//不必需,表示触摸位移与被运动属性映射关系，默认值是1
    spring: true, //不必需,是否有回弹效果。默认是true
    step: 45,//用于校正到step的整数倍
    bindSelf: false,
    initialVaule: 20,
    change:function(value){}, //不必需，属性改变的回调。alloytouch.css版本不支持该事件
    touchStart:function(evt, value){  },
    touchMove:function(evt, value){  },
    touchEnd:function(evt,value){  },
    tap:function(evt, value){

    },
    pressMove:function(evt, value){

    },
    animationEnd:function(value){  } //运动结束
})