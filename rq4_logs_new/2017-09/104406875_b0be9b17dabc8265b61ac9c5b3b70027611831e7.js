var canvas = document.getElementById('canvas');
var c = canvas.getContext('2d');

canvas.width = window.innerWidth;
canvas.height = window.innerHeight;

c.lineJoin = "round";
c.lineCap = "round";
c.lineWidth = 80;

var isDrawing = false;
var lastX = 0;
var lastY = 0;
var hue = 0;
var direction = 0;
var col = document.querySelector('#col');
col.addEventListener('click', function(){
	c.globalCompositeOperation = "color";
});
var xor = document.querySelector('#xor');
xor.addEventListener('click', function(){
	c.globalCompositeOperation = "xor";
})
var diff = document.querySelector('#diff');
diff.addEventListener('click', function(){
	c.globalCompositeOperation = "difference";
})
var def = document.querySelector('#def');
def.addEventListener('click', function(){
	c.globalCompositeOperation = "source-over";
});
var h = document.querySelector('#hue');
h.addEventListener('click', function(){
	c.globalCompositeOperation = "hue";
});



function draw(e){
	if(!isDrawing) return; 
	c.strokeStyle = `hsl(${hue},100%,50%)`;
	c.beginPath();
	c.moveTo(lastX, lastY);
	c.lineTo(e.offsetX, e.offsetY);
	c.stroke();

	[lastX, lastY] = [e.offsetX, e.offsetY];

	hue++;
	if(hue > 360){
		hue = 0;
	}
}

window.addEventListener('mousemove', draw);
window.addEventListener('mousedown', (e) => {
	isDrawing = true;
	[lastX, lastY] = [e.offsetX, e.offsetY];
});
window.addEventListener('mouseup', () => isDrawing = false);
window.addEventListener('mouseout', () => isDrawing = false);