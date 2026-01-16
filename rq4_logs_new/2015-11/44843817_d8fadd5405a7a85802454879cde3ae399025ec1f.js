var main = function() {
	function epil() {
		$('.back').addClass('red');
		$('.back').removeClass('blue');
		setTimeout(function() {apil();}, 10);
	};
	function apil() {
		$('.back').addClass('blue');
		$('.back').removeClass('red');
		setTimeout(function() {epil();}, 10);
	};
	epil();
	function circle(dvx, dvy, vx, vy, x, y, count) {
		var up = document.getElementById("upload");
		var xx = x + vx;
		var yy = y + vy;
		var vvx = vx + dvx;
		var vvy = vy + dvy;
		var ddx = dvx;
		var ddy = dvy;
		var cc = count + 1;
		if (count % 30 === 0) {ddx = -ddx;};
		if ((count + 15) % 30 === 0) {ddy = -ddy;};
		up.style.marginLeft = x + "px";
		up.style.marginTop = y + "px";
		setTimeout(function() {circle(ddx, ddy, vvx, vvy, xx, yy, cc);}, 30);
	}
	circle(1, 1, 14, -1, 0, 0, 0);
};

$(document).ready(main);

function something() {
	alert("SOMETHING");
};