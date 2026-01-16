
/*请在该区域内声明或者获取所要使用的全局变量*/
/********************************************begin************************************/

/*Global Variable Area */
//作为根的元素
var root = document.getElementById("bd");
//class值为container的元素
var container = root.firstElementChild;
//class值为wrap的元素
var wrap = container.firstElementChild;
//图片右下角显示的按钮
var buttons = wrap.nextElementSibling;
//向右的箭头
var toNextArrow = container.lastElementChild;
//向左的箭头
var toPrevArrow = toNextArrow.previousElementSibling;
//用来实现图片切换的参数
var currentLeft = -600;
//标记当前图片序数的变量
var currentImg = 0;
//存储轮播的一个变量
var doWrap;


/*********************************************end*************************************/



/* 任务一
 * 请参考css中的style参数、html中的内容、下方的效果要求，然后在下面区域内编写代码。
 * 效果要求：
 * ①点击左箭头prev和右箭头next的时候，可以切换到前一张图片和下一张图片。（左右箭头为html中的a标签）
 * ②每切换一张图片，右下角的数字标记对应变化。
 *      如：一开始，第1张图片显示出来，右下角的1-5的数值中，数值1位红色，2-4为绿色，表示当前显示第1张图片。
 *      点击next箭头，图片切换到第2张，同时，右下角红色数值从1切换为2，数值1,3,4,5为绿色。
 * ③当当前图片为第1张时，点击prev箭头，切换到第5张图片，且数值5置为红色。
 * 当当前图片为第5张时，点击next箭头，切换到第1张图片，且数值1置为红色。
 * ④切换图片的过程不要求，可直接切换，也可动画切换，但要求保证一定的切换动画合理性，不能出去明显的衔接不当。
 * ⑤本部分只能使用原生JS。
 */
/********************************************begin************************************/

/*Code Here*/
//向前切换图片的函数
function toPrev() {
	buttons.children[currentImg].classList.remove("on");
	if(currentLeft == -600){
		currentLeft = -3000;
		currentImg = 4;
	} else {
		currentLeft += 600;
		currentImg -= 1; 
	}
	wrap.style.left = currentLeft + "px";
	buttons.children[currentImg].classList.add("on");
}
//向后切换图片的函数
function toNext() {
	buttons.children[currentImg].classList.remove("on");
	if(currentLeft == -3000){
		currentLeft = -600;
		currentImg = 0;
	} else {
		currentLeft -= 600;
		currentImg += 1;
	}
	wrap.style.left = currentLeft + "px";
	buttons.children[currentImg].classList.add("on");
}
toPrevArrow.addEventListener("click", toPrev);
toNextArrow.addEventListener("click", toNext);



/*********************************************end*************************************/



/* 任务二
 * 请参考css中的style参数、html中的内容、下方的效果要求，然后在下面区域内编写代码。
 * 效果要求：
 * ①轮播可以自动播放，切换图片间隔为2s，每一次切换的效果与点击next箭头的效果一致。
 * ②当鼠标移入轮播区域内时，停止自动播放。
 * ③当鼠标不在轮播区域内时，开始自动播放。
 * ④页面刚加载完成时，如果鼠标不在轮播区域内，自动开始自动播放；否则，等待直到鼠标移出轮播区域，再进行自动播放。
 * ⑤本部分只能使用原生JS。
 */
/********************************************begin************************************/

/*Code Here*/
//开始轮播的函数
function startWrap() {
	doWrap = setInterval(toNext, 2000);
}
//停止轮播的函数
function stopWrap() {
	clearInterval(doWrap);
}
//在页面加载完成时将上面两个函数绑定到container的两个事件上
window.onload = function () {
	startWrap();
	container.addEventListener("mouseover", stopWrap);
	container.addEventListener("mouseout", startWrap);
}


/*********************************************end*************************************/



/* 任务三
 * 请参考css中的style参数、html中的内容、下方的效果要求，然后在下面区域内编写代码。
 * 效果要求：
 * ①点击右下角的任意一个数值，能够切换到对应图片，且相应数值变为红色。
 * ②进行①操作过后，是否自动播放，其规则与上一个任务一致。
 * ③本部分只能使用原生JS。
 */
/********************************************begin************************************/

/*Code Here*/
//主函数，思路是利用循环重复上面的toNext()函数和toPrev()函数来实现图片跳转
function clickBtn() {
	//根据当前图片序数和目标图片序数来计算循环的次数以及方向
	let imgOn = getCurrentImg();
	let imgTarget = parseInt(this.innerHTML);
	let delta = 0;
	if(imgOn < imgTarget) {
		delta = imgTarget - imgOn;
		for(let i = 0; i < delta; i++){
			toNext();
		}
	}
	if(imgOn > imgTarget) {
		delta = imgOn - imgTarget;
		for(let i = 0; i < delta; i++){
			toPrev();
		}
	}
}
//辅助函数，获取当前图片的序数
function getCurrentImg() {
	let leftOn = wrap.style.left.substring(0, wrap.style.left.length - 2);
	let imgOn = 0;
	leftOn = parseInt(leftOn);
	imgOn = leftOn / (-600);
	return imgOn;
}
//为每个按钮添加上点击事件
for(let i = 0; i < 5; i++) {
	buttons.children[i].addEventListener("click", clickBtn);
}


/*********************************************end*************************************/


/*任务四
 * 请参考css中的style参数、html中的内容、下方的效果要求，然后在下面区域内编写代码。
 * 效果要求：
 * ①点击某一非表头的单元格，可以编辑其内容，编辑完毕后点击其他部位，可以在界面上显示修改后的内容。
 * ②点击单元格后，光标自动定位于单元格的首个字符或者汉字前。
 * ③本部分可以使用jQuery，也可以使用原生JS。
 */
/********************************************begin************************************/

/*Code Here*/
//用jQuery完成这部分的内容
$(document).ready(function() {
	//设置td的点击事件
	$("td").click(function() {
		let td = $(this);
		let content = td.html();
		//将td变成一个input进行输入
		td.html("<input type = 'text' class = 'currentInput' >");
		let input = $('.currentInput');
		input.attr({"value": content});
		//input聚焦
		input.focus();
		//将input的点击事件设置成false
		input.click(function() {
			return false;
		});
		//失去焦点时将input变回文本
		input.blur(function() {
			let newContent = input.val();
			td.html(newContent);
		});
	});
});

/*********************************************end*************************************/