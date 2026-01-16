//$()   参数
/*
	函数      window.onload
	string   id  class  TagName
	对象  window this document
*/
/**
 *            工具类 start
 */

//跨浏览器添加事件
function addEvent(obj, type, fn){
	if(obj.addEventListener){
		obj.addEventListener(type, fn, false);
	}else if(obj.attachEvent){ //IE this传递不过去  去重
		obj.attachEvent("on" + type, function(){
			fn.call(obj);
		});
	}
}

//跨浏览器删除事件
function removeEvent(obj, type, fn){
	if(obj.removeEventListener){
		obj.removeEventListener(type, fn);
	}else if(obj.detachEvent){
		obj.detachEvent("on" + type, fn);
	}
}
//通过class获取元素,遍历所有标签，找出适合的,放到一个数组中返回出来
function getByClass(oParent,sClass){
	var suoyoude=oParent.getElementsByTagName("*");
	var aResult=[];
	for (var i=0; i<suoyoude.length; i++) {
		if (suoyoude[i].className == sClass) {
			aResult.push(suoyoude[i]);
		}
	}
	return aResult;
}
function getStyle(element,attr){
	if (element.currentStyle) {
		return element.currentStyle[attr];
	}else{
		return getComputedStyle(element)[attr];
	}
}
//将数组的每一项加到另一个数组后面concat
function appendArr(arr1,arr2){
	for (var i=0; i<arr2.length; i++) {
		arr1.push(arr2[i]);
	}
}


/**
 *                 工具类结束
 */
//$()选择器
function Mquery(vArg){
	this.elements=[];//存放被选择元素的数组
	switch (typeof vArg){
		case "function":
			addEvent(window,"load",vArg);
			break;
		case "string":
			switch (vArg.charAt(0)){
				case "#":
					var obj=document.getElementById(vArg.substring(1));
					this.elements.push(obj);
					break;
				case ".":
					this.elements=getByClass(document,vArg.substring(1));
					break;
				default:
					this.elements=document.getElementsByTagName(vArg.substring(1));
					break;
			}
			break;
		default:
			this.elements=vArg;
			break;
	}
}
function $(vArg){
	return new Mquery(vArg);
}

Mquery.prototype.click=function(fn){
	for (var i=0; i<this.elements.length; i++) {
		addEvent(this.elements[i],"click",fn);
	}
	return this;
}
Mquery.prototype.show=function(){
	for (var i=0; i<this.elements.length; i++) {
		this.elements[i].style.display="block";
	}
	return this;
}
Mquery.prototype.hide=function(){
	for (var i=0; i<this.elements.length; i++) {
		this.elements[i].style.display="none";
	}
	return this;
}
//hover 同时添加out,mouseover,事件
Mquery.prototype.hover=function(fnOver, fnOut){
	for (var i=0; i<this.elements.length; i++) {
		addEvent(this.elements[i],"mouseover",fnOver);
		addEvent(this.elements[i],"mouseout",fnOut);
	}
	return this;
}
//【注】：语法格式：有一个参数，获取，两个，设置,{}
Mquery.prototype.css=function(attr,value){
	if (arguments.length==2) {
		for (var i=0; i<this.elements.length; i++) {
			this.elements[i].style[attr]=value;
		}
	}else{
		if (typeof arguments[0]=="string") {
			return getStyle(this.elements[0],attr);
		}else{//json
			for (var i=0; i<this.elements.length; i++) {
				for (var key in attr){
					this.elements[i].style[key]=attr[key];
				}
			}
		}
	}
	return this;
}
Mquery.prototype.attr=function(attr,value){
	if (arguments.length ==2) {
		for (var i=0; i<this.elements.length;i++ ) {
			this.elements[i][attr]=value;
		}
	}else{
		return this.elements[0][attr];
	}
	return this;
}
Mquery.prototype.eq=function(n){
	return $(this.elements[n]); 
}
Mquery.prototype.find=function(str){
	var aResult=[];
	for (var i=0; i<this.elements.length; i++) {
		switch (str.charAt(0)){
			case ".":
				var aEle=getByClass(this.elements[i] , str.substring(1));
				aResult=aResult.concat(aEle);
				break;
			default:
				aEle=this.elements[i].getElementsByTagName(str);
				appendArr(aResult ,aEle);
				break;
		}
	}
	var newMquery=$();
	newMquery.elements=aResult;
	return newMquery;
}
Mquery.prototype.index=function(){
	
}