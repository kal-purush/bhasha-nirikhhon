var score = 0;

var btn = document.getElementById('btn');
btn.addEventListener("click", function(){ 
	score++;
	document.getElementById("score").innerHTML = "Clicks: 0";
});