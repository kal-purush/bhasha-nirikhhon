var img = document.getElementById('cpu');
var imgnumber=2;
var image = document.getElementById('process');
var video = document.getElementById('video');
function change () {
	if(imgnumber==10)
		imgnumber=1;
	img.src="frames/cpu"+imgnumber+".jpg";
	image.src="frames/frame"+imgnumber+".mp4";
	video.load();
	video.play();
	console.log("yes");
	imgnumber++;
}