document.querySelector(".js-go").addEventListener('click',function(){
	//alert("test");
	var input = document.querySelector("input").value;
	var userinput= getUseInput();
	searchGiphy(userinput);
	//console.log(input);
	  //pushToDOM(input);
});
document.querySelector(".js-go").addEventListener('click',function(){
	//alert("test");
	var input = document.querySelector("input").value;
	var userinput= getUseInput();
	searchGiphy(userinput);
	//console.log(input);
	  //pushToDOM(input);
});
document.querySelector(".js-userinput").addEventListener('keyup',function(e){
	// alert("test");
	var input = document.querySelector("input").value;
	//console.log(input);
	if (e.which ===13){
		var userinput = getUseInput();
		searchGiphy(userinput);
	  //pushToDOM(input);
	}
});
function getUseInput(){
	var inputvalue = document.querySelector(".js-userinput").value;
	return inputvalue;
}
/*2. do the data stuff with the api */
function searchGiphy(searchquery){
	var url = "http://api.giphy.com/v1/gifs/search?api_key=dc6zaTOxFJmzC&q=" + searchquery;
//AJAX Request
var GiphyAJAXCall = new XMLHttpRequest();
GiphyAJAXCall.open( 'GET', url );
GiphyAJAXCall.send();
GiphyAJAXCall.addEventListener('load',function(data){
	var actualdata = data.target.response;
	console.log(actualdata);
	pushToDOM(actualdata);
});
}
/* 3. show me the gif */
function pushToDOM(response) {
	response = JSON.parse(response);
	var images = response.data;
	var container = document.querySelector('.js-container');
	container.innerHTML = "";
  
  images.forEach(function(image){
  	var src = image.images.fixed_height.url;
  	var container = document.querySelector(".js-container");
	container.innerHTML += "<img src='"+ src +"' class='container-image' />";
  });
  // var  imageUrl = response.data[7].images.fixed_height.url;
  
}