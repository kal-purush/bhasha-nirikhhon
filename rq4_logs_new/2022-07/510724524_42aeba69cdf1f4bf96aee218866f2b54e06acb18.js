function circleAnimation() {
	let consElem = document.getElementById("circleContent");
	let elemForAnimation = document.getElementById("circle");
	let elemCentr = elemForAnimation.style.height/2

	return function() {
		let cords = consElem.getBoundingClientRect();
	    if(cords.top <= -10){
	    	elemForAnimation.classList.add('start');
	    	elemForAnimation.classList.remove('revers');
	  	} 
	  	else {
	  		if(elemForAnimation.classList.contains('start')) {elemForAnimation.classList.add('revers')}
	  		elemForAnimation.classList.remove('start');;
	  	}
	}
}

function swiperAnimation(type) {
	swiperAnimation.activeChildren = 0;
	let swiper =  document.querySelector(".swiperLists");
	let childrenCount = swiper.children.length;
	let move = () => (-300 * swiperAnimation.activeChildren) + "px";

	if(type == "prev") {
		return function() {
			if(swiperAnimation.activeChildren > 0) {
				swiperAnimation.activeChildren--;
				swiper.style.left = move();
			}
		} 
	}

	if(type == "next") {
		return function() {
			if(swiperAnimation.activeChildren < childrenCount-1) {
				swiperAnimation.activeChildren++;
				swiper.style.left = move();
			}
		} 
	}
}

window.addEventListener('scroll', circleAnimation());

document.querySelector(".additionalSettings .controlButtons .prev").addEventListener("click", swiperAnimation("prev"));
document.querySelector(".additionalSettings .controlButtons .next").addEventListener("click", swiperAnimation("next"));