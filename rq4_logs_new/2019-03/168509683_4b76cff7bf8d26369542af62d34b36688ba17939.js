/*----------------------------------------*\
  256x256x256 - script.js
  @author Evrard Vincent (vincent@ogre.be)
  @Date:   2018-02-27 12:27:03
  @Last Modified time: 2019-01-31 12:06:22
\*----------------------------------------*/

/*document.addEventListener("DOMContentLoaded", setup);


function tapHandler (event){
	top.postMessage('SUCCESS', '*');
	console.log("tapHandler");
		var btnToHide = document.querySelector(".trigger2");
	btnToHide.classList.add("hidden");
}

function setup(){
	var trigger1 = interact('.trigger1');
	trigger1.on("tap", step1);
	
}

function step1(event){
	var btnToHide = document.querySelector(".trigger1");
	btnToHide.classList.add("hidden");
	var btnToShow = document.querySelector(".trigger2");
	btnToShow.classList.remove("hidden");

	var trigger2 = interact('.trigger2');
	trigger2.on("tap", tapHandler);
} */

interact('.tap-target')
  .on('tap', function (event) {
    event.currentTarget.classList.toggle('switch-bg');
    event.preventDefault();
  })
  .on('doubletap', function (event) {
    event.currentTarget.classList.toggle('rotate');
    event.currentTarget.classList.remove('large');
    event.preventDefault();
  })

function doubleTapHandler (event){
	top.postMessage('SUCCESS', '*');
	console.log("doubleTapHandler");
}
  