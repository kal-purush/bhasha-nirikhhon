var done = document.getElementById('p');
var image = document.getElementById('avtar');
function create(){
	console.log(document.getElementById("in").value);
	var add="https://robohash.org/"+document.getElementById("in").value+"?300x300";
	done.textContent="This is your robo-avtar dear "+document.getElementById("in").value +"";
	image.src=add;
}