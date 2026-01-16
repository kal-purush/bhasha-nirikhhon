var mainSliderInfo ={

	0 : {

		title : "Donec faucibus ultricies congue",
		images : ["images/mainSlider/0/1.jpg","images/mainSlider/0/2.jpg", "images/mainSlider/0/3.jpg"]
	},

	1 : {


		title : "Donec faucibus ultricies congue 1",
		images : ["images/mainSlider/1/1.jpg","images/mainSlider/1/2.jpg", "images/mainSlider/1/3.jpg"]
	},

	2 : {

		title : "Donec faucibus ultricies congue 2",
		images : ["images/mainSlider/2/1.jpg","images/mainSlider/2/2.jpg", "images/mainSlider/2/3.jpg"]

	}


};


var relatedSliderInfo = {
	 0 : {
	 		images : ["images/relatedSlider/0/0.jpg","images/relatedSlider/0/1.jpg","images/relatedSlider/0/2.jpg"]
	 },

	 1 : {
	 		images : ["images/relatedSlider/1/0.jpg","images/relatedSlider/1/1.jpg","images/relatedSlider/1/2.jpg","images/relatedSlider/1/3.jpg"]
	 },

	 2 : {
	 	 images : ["images/relatedSlider/2/0.jpg","images/relatedSlider/2/1.jpg"]
	 }

}


var searchEntity = document.getElementById("searchEntity");
var searchHolder =document.getElementById("seachHolder");

var currentMainSlider = 0;
var mainSliderTitle = document.getElementById("mainSliderTitle");
var currentMainImage = 0;
var currentRelatedSlider = 0;
var mainSliderContainer = document.getElementById("mainSliderContainer");
mainSliderContainer.style.position = "relative";

var indicatorContainer = document.getElementById("indicatorContainer");
var relatedSliderContainer = document.getElementById("relatedSliderContainer");


var mainSliderLeft = document.getElementById("mainSliderRight");
var mainSliderRight = document.getElementById("mainSliderLeft");


var relatedSliderLeft = document.getElementById("relatedSliderLeft");
var relatedSliderRight = document.getElementById("relatedSliderRight");

var leftControl = document.getElementById("leftControl");
var rightControl = document.getElementById("rightControl");
var marginValueNext = 0;
var marginValuePrev = 0;
var pressed = false;
var indicatorList = [];
var opacityFadeout = 1;
var opacityFadeIn = 0.05;
var fadeOut = true;
var fadeIn = false; 
var marginValueCurrent = 0;
const displacement = 1170;
const default_relatedSlider = "images/relatedSlider/default.jpg";


initMainSlider();

function initMainSlider(){

	mainSliderTitle.textContent = mainSliderInfo[currentMainSlider].title;
	for (var i =0; i < mainSliderInfo[currentMainSlider].images.length ; i++) {
			var imagesDivContainer = document.createElement("div");
			if (currentMainImage == 0) {
				imagesDivContainer.className = "item active"; 
			}
			imagesDivContainer.className = "item"; 
			imagesDivContainer.style.position = "absolute";
			imagesDivContainer.style.left = i*displacement + "px";

			var images  = document.createElement("img");
			images.src =   mainSliderInfo[currentMainSlider].images[i];

			imagesDivContainer.appendChild(images);
			mainSliderContainer.appendChild(imagesDivContainer);


			var indicator = document.createElement("li");
			indicator.className = "slider-image-indicator";
			indicatorList.push(indicator);
			indicatorContainer.appendChild(indicator);

	}





	for (var i =0; i <relatedSliderInfo[currentRelatedSlider].images.length;i++) {

		var listContainer =  document.createElement("li");

		listContainer.className = "related-products-list clearfix";

		var divContainer = document.createElement("div");
		divContainer.className = "products-container";

		var images = document.createElement("img");
		images.src = relatedSliderInfo[currentRelatedSlider].images[i];
		images.width = 270;
		images.height = 270;

		var divContainerProductPop =  document.createElement("div");
		divContainerProductPop.className = "products-popup";

		var divContainerProductLinks = document.createElement("div");
		divContainerProductLinks.className = "products-links";

		divContainerProductLinks.innerHTML = ' <ul><li><a href="#" target="_blank"><i class="fa fa-eye" aria-hidden="true"></i></a></li><li><a href="#" target="_blank"><i class="fa fa-link" aria-hidden="true"></i></a></li></ul>';

		var divContainerProductDescription = document.createElement("div");
		divContainerProductDescription.className = "related-product-description";
		divContainerProductDescription.textContent = "Image " +(i+1);


		listContainer.appendChild(divContainer);
		divContainer.appendChild(images);
		divContainer.appendChild(divContainerProductPop);
		divContainerProductPop.appendChild(divContainerProductLinks);
		divContainerProductLinks.appendChild(divContainerProductDescription);	
		relatedSliderContainer.appendChild(listContainer);

	}


	if (relatedSliderInfo[currentRelatedSlider].images.length <4) {

		for(var i =0 ; i< 4-relatedSliderInfo[currentRelatedSlider].images.length; i++){
			var listContainer =  document.createElement("li");

			listContainer.className = "related-products-list clearfix";

			var divContainer = document.createElement("div");
			divContainer.className = "products-container";

			var images = document.createElement("img");
			images.src = default_relatedSlider;
			images.width = 270;
			images.height = 270;

			var divContainerProductPop =  document.createElement("div");
			divContainerProductPop.className = "products-popup";

			var divContainerProductLinks = document.createElement("div");
			divContainerProductLinks.className = "products-links";

			divContainerProductLinks.innerHTML = ' <ul><li><a href="#" target="_blank"><i class="fa fa-eye" aria-hidden="true"></i></a></li><li><a href="#" target="_blank"><i class="fa fa-link" aria-hidden="true"></i></a></li></ul>';

			var divContainerProductDescription = document.createElement("div");
			divContainerProductDescription.className = "related-product-description";
			divContainerProductDescription.textContent = "Image  default" ;


			listContainer.appendChild(divContainer);
			divContainer.appendChild(images);
			divContainer.appendChild(divContainerProductPop);
			divContainerProductPop.appendChild(divContainerProductLinks);
			divContainerProductLinks.appendChild(divContainerProductDescription);	
			relatedSliderContainer.appendChild(listContainer);




		}



	}


	checkControls();
	setIndicator();






}



function setIndicator(){
	var i = 0;
	indicatorList.forEach(function(currentIndicator){

		if (i == currentMainImage) {

				currentIndicator.className ="current";
		}else{
				currentIndicator.className = "slider-image-indicator"
		}

		i++;
	});


}


function searchMouse(object){
 
	object.style.display = "none";
}
 
searchEntity.addEventListener("click", function () {
	// body...

	 searchHolder.style.display = "block";
});


mainSliderLeft.addEventListener("click", function(){

	if (pressed == false) {
		pressed = true;
		opacityFadeout = 1;
		opacityFadeIn = 0.05;
		fadePrevAnimation();

	}

});


mainSliderRight.addEventListener("click",function(){


	if (pressed == false) {

		pressed = true;
		opacityFadeout = 1;
		opacityFadeIn = 0.05;
		fadeNextAnimation();	
	}

});



	
leftControl.addEventListener("click",function(){
		
		if (pressed == false) {
			pressed = true;
			marginValuePrev = - displacement*(currentMainImage-1);
			marginValueCurrent = - displacement*(currentMainImage);
			slidePrevAnimation();
		}
		
		


});


rightControl.addEventListener("click",function(){

	if (pressed == false) {
			pressed = true;
			marginValueNext = - displacement*(currentMainImage+1);
			marginValueCurrent = - displacement*(currentMainImage);
			slideNextAnimation();

	}

});



relatedSliderLeft.addEventListener("click",function(){
	if (pressed == false) {
		pressed = true;
		opacityFadeout = 1;
		opacityFadeIn = 0.05;
		fadePrevRelatedSliderAnimation();
	}

});




relatedSliderRight.addEventListener("click",function(){
	 

	if (pressed == false) {
		pressed = true;
		opacityFadeout = 1;
		opacityFadeIn = 0.05;
		fadeNextRelatedSliderAnimation();
	}


});



function checkControls(){

	if (currentMainImage ==0) {
		leftControl.style.display = "none";
	}else if (currentMainImage == mainSliderInfo[currentMainSlider].images.length-1) {
		rightControl.style.display = "none";
	}else{
		leftControl.style.display = "block";
		rightControl.style.display = "block";
	}


	if (currentMainSlider == 0) {
			mainSliderLeft.style.display ="none";
	}else if (currentMainSlider == Object.keys(mainSliderInfo).length-1 ) {
		mainSliderRight.style.display = "none";
	}else{
		mainSliderLeft.style.display = "block";
		mainSliderRight.style.display = "block";
	}


	if (currentRelatedSlider == 0) {

		relatedSliderLeft.style.display = "none";

	}else if (currentRelatedSlider == Object.keys(relatedSliderInfo).length-1) {
		relatedSliderRight.style.display = "none";

	}else{
		relatedSliderLeft.style.display = "block";
		relatedSliderRight.style.display = "block";

	}

}



function fadeNextAnimation(){

	if (opacityFadeout > opacityFadeIn) {

			opacityFadeout -= 0.05;
			mainSliderContainer.style.opacity = opacityFadeout;
      setTimeout(function(){fadeNextAnimation()},10);
	}else{

	
			mainSliderContainer.style.opacity = 1;
			pressed = false;
			removeSlider();
			currentMainSlider ++;
			refreshSlider();
			resetMainSlider();
			

	}


}



function removeSlider(){

		while(mainSliderContainer.firstChild) {
    	mainSliderContainer.removeChild(mainSliderContainer.firstChild);
		}

		while(indicatorContainer.firstChild) {
    	indicatorContainer.removeChild(indicatorContainer.firstChild);
		}

		indicatorList = [];
}


function refreshSlider(){

	mainSliderTitle.textContent = mainSliderInfo[currentMainSlider].title;
	for (var i =0; i < mainSliderInfo[currentMainSlider].images.length ; i++) {
			var imagesDivContainer = document.createElement("div");
			if (currentMainImage == 0) {
				imagesDivContainer.className = "item active"; 
			}
			imagesDivContainer.className = "item"; 
			imagesDivContainer.style.position = "absolute";
			imagesDivContainer.style.left = i*displacement + "px";

			var images  = document.createElement("img");
			images.src =   mainSliderInfo[currentMainSlider].images[i];

			imagesDivContainer.appendChild(images);
			mainSliderContainer.appendChild(imagesDivContainer);


			var indicator = document.createElement("li");
			indicator.className = "slider-image-indicator";
			indicatorList.push(indicator);
			indicatorContainer.appendChild(indicator);

	}


}



function resetMainSlider(){
	currentMainImage = 0;
	mainSliderContainer.style.left = "0px";
	opacityFadeout = 1;
	opacityFadeIn = 0.05;
	checkControls();
	setIndicator();
	
}




function fadePrevAnimation(){
	if (opacityFadeout > opacityFadeIn) {

			opacityFadeout -= 0.05;
			mainSliderContainer.style.opacity = opacityFadeout;
      setTimeout(function(){fadePrevAnimation()},10);
	}else{

	
			mainSliderContainer.style.opacity = 1;
			pressed = false;
			removeSlider();
			currentMainSlider --;
			refreshSlider();
			resetMainSlider();
			

	}



}








 function slideNextAnimation(){

 	if (marginValueCurrent > marginValueNext) {
      marginValueCurrent -= 10;
      mainSliderContainer.style.left = marginValueCurrent+"px";
     setTimeout(function(){slideNextAnimation()},10);
   }else{
   		currentMainImage++;
   		pressed = false;
   		checkControls();
   		setIndicator();
   } 

 }

 function slidePrevAnimation(){
 			
 	if (marginValueCurrent < marginValuePrev) {
   		marginValueCurrent += 10;
      mainSliderContainer.style.left = marginValueCurrent+"px";
      setTimeout(function(){slidePrevAnimation()},10);
 		}else{
 		currentMainImage--;
   		pressed = false;
   		checkControls();
   		setIndicator();

 		}
 	  	

 }


 function fadeNextRelatedSliderAnimation(){


 	if (opacityFadeout > opacityFadeIn) {

			opacityFadeout -= 0.05;
			relatedSliderContainer.style.opacity = opacityFadeout;
      		setTimeout(function(){fadeNextRelatedSliderAnimation()},10);
	}else{

	
			relatedSliderContainer.style.opacity = 1;
			pressed = false;
			removeRelatedSlider();
			currentRelatedSlider ++;
			refreshRelatedSlider();
			checkControls();
			

	}

 }


  function fadePrevRelatedSliderAnimation(){
 	if (opacityFadeout > opacityFadeIn) {

			opacityFadeout -= 0.05;
			relatedSliderContainer.style.opacity = opacityFadeout;
      		setTimeout(function(){fadePrevRelatedSliderAnimation()},10);
	}else{

	
			relatedSliderContainer.style.opacity = 1;
			pressed = false;
			removeRelatedSlider();
			currentRelatedSlider --;
			refreshRelatedSlider();
			checkControls();
			

	}


 }




  function removeRelatedSlider(){

	  	while(relatedSliderContainer.firstChild) {
	    	relatedSliderContainer.removeChild(relatedSliderContainer.firstChild);
		}

  }


  function refreshRelatedSlider() {

	for (var i =0; i <relatedSliderInfo[currentRelatedSlider].images.length;i++) {

		var listContainer =  document.createElement("li");

		listContainer.className = "related-products-list clearfix";

		var divContainer = document.createElement("div");
		divContainer.className = "products-container";

		var images = document.createElement("img");
		images.src = relatedSliderInfo[currentRelatedSlider].images[i];
		images.width = 270;
		images.height = 270;

		var divContainerProductPop =  document.createElement("div");
		divContainerProductPop.className = "products-popup";

		var divContainerProductLinks = document.createElement("div");
		divContainerProductLinks.className = "products-links";

		divContainerProductLinks.innerHTML = ' <ul><li><a href="#" target="_blank"><i class="fa fa-eye" aria-hidden="true"></i></a></li><li><a href="#" target="_blank"><i class="fa fa-link" aria-hidden="true"></i></a></li></ul>';

		var divContainerProductDescription = document.createElement("div");
		divContainerProductDescription.className = "related-product-description";
		divContainerProductDescription.textContent = "Image " +(i+1);


		listContainer.appendChild(divContainer);
		divContainer.appendChild(images);
		divContainer.appendChild(divContainerProductPop);
		divContainerProductPop.appendChild(divContainerProductLinks);
		divContainerProductLinks.appendChild(divContainerProductDescription);	
		relatedSliderContainer.appendChild(listContainer);

	}


	if (relatedSliderInfo[currentRelatedSlider].images.length <4) {

		for(var i =0 ; i< 4-relatedSliderInfo[currentRelatedSlider].images.length; i++){
			var listContainer =  document.createElement("li");

			listContainer.className = "related-products-list clearfix";

			var divContainer = document.createElement("div");
			divContainer.className = "products-container";

			var images = document.createElement("img");
			images.src = default_relatedSlider;
			images.width = 270;
			images.height = 270;

			var divContainerProductPop =  document.createElement("div");
			divContainerProductPop.className = "products-popup";

			var divContainerProductLinks = document.createElement("div");
			divContainerProductLinks.className = "products-links";

			divContainerProductLinks.innerHTML = ' <ul><li><a href="#" target="_blank"><i class="fa fa-eye" aria-hidden="true"></i></a></li><li><a href="#" target="_blank"><i class="fa fa-link" aria-hidden="true"></i></a></li></ul>';

			var divContainerProductDescription = document.createElement("div");
			divContainerProductDescription.className = "related-product-description";
			divContainerProductDescription.textContent = "Image  default" ;


			listContainer.appendChild(divContainer);
			divContainer.appendChild(images);
			divContainer.appendChild(divContainerProductPop);
			divContainerProductPop.appendChild(divContainerProductLinks);
			divContainerProductLinks.appendChild(divContainerProductDescription);	
			relatedSliderContainer.appendChild(listContainer);




		}

	}

   
  }