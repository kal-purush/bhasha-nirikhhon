/* $(window).load(function(){
	document.getElementById("loading").style.display = "none";
}); */

var i = setInterval(function () {
    
    clearInterval(i);
  
    // O código desejado é apenas isto:
    document.getElementById("loading").style.display = "none";
    document.getElementById("content").style.display = "block";

}, 2000);