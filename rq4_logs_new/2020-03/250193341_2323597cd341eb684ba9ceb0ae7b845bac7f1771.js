var css = document.querySelector("h3");
var color1 = document.querySelector(".color1");
var color2 = document.querySelector(".color2");
var body = document.getElementById("gradient");
const rnd_btn = document.getElementById("random");

function setGradient() {
	body.style.background = 
	"linear-gradient(to right, " 
	+ color1.value 
	+ ", " 
	+ color2.value 
	+ ")";

	css.textContent = getBodyBackgroundCSS() + ";";
}

function getBodyBackgroundCSS() {
	const body_style = getComputedStyle(body);
	return body_style.backgroundImage;
}

function getBodyBackgroundColor() {
	const body_style = getComputedStyle(body);
	return body_style.backgroundImage.match(/rgb\(\d+, \d+, \d+\)/g);
}


function rgb2Hex(color) {
	const [r, g, b] = color.substr(4).split(")")[0].split(",");

	let hex_r = (+r).toString(16), 
		hex_g = (+g).toString(16),
		hex_b = (+b).toString(16);
	

	if (hex_r.length == 1) hex_r = "0" + hex_r;
	if (hex_g.length == 1) hex_g = "0" + hex_g;
	if (hex_b.length == 1) hex_b = "0" + hex_b;
	

	return "#" + hex_r + hex_g + hex_b;
}

function setInitialBody() {
	const colors_rgb = getBodyBackgroundColor();
	const colors_hex = colors_rgb.map( (cur) => rgb2Hex(cur) );
	color1.value = colors_hex[0]; color2.value = colors_hex[1];

	css.textContent = getBodyBackgroundCSS();

}

function genRandomInt() {
	return Math.floor(Math.random() * (0xffffff + 1));
}

function generateRandomColor() {
	const first_color = genRandomInt();
	const second_color = genRandomInt();

	color1.value = "#" + first_color.toString(16); color2.value = "#" + second_color.toString(16);

	setGradient();

}

window.addEventListener("load", setInitialBody);

color1.addEventListener("input", setGradient);

color2.addEventListener("input", setGradient);

rnd_btn.addEventListener("click", generateRandomColor);