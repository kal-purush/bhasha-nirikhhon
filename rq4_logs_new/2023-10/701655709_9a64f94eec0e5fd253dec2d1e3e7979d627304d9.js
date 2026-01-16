const numPoints = 80;
const numFollowing = 3;
const radiusFactor = 6;
const velFactor = 0.4;
let canvas, ctx, points, t;

function load() {
	canvas = document.getElementById('canvas');
	canvas.width = innerWidth - 40;
	canvas.height = innerHeight - 80;
	ctx = canvas.getContext('2d');

	start();
	requestAnimationFrame(draw);
}

function start() {
	ctx.fillStyle = '#020';
	ctx.fillRect(0, 0, canvas.width, canvas.height);
	t = 0;
	points = [];
	for (let i = 0; i < numPoints; i++) {
		points.push({
			pos: {
				x: Math.floor(Math.random() * canvas.width),
				y: Math.floor(Math.random() * canvas.height),
			},
			vel: {
				x: 0,
				y: 0,
			},
			following: [],
			color: hsvToRgb(Math.random(), 0.8, 1),
			radiusFreq: 0.1 * (Math.random() + 0.8),
			radius: 0,
		});
	}
	for (const p of points) {
		for (let i = 0; i < numFollowing; i++) {
			let f;
			do {
				f = points[Math.floor(Math.random() * points.length)];
			} while (f == p);
			p.following.push(f);
		}
	}
}

function step() {
	for (const p of points) {
		for (const f of p.following) {
			const dx = p.pos.x - f.pos.x;
			const dy = p.pos.y - f.pos.y;
			const distSq = dx ** 2 + dy ** 2;
			p.vel.x -= (velFactor * dx) / distSq;
			p.vel.y -= (velFactor * dy) / distSq;
		}
		// console.log(p.vel);
	}
	for (const p of points) {
		p.pos.x += p.vel.x;
		p.pos.y += p.vel.y;

		if (p.pos.x < 0 || p.pos.x > canvas.width) {
			p.vel.x *= -1;
		}
		if (p.pos.y < 0 || p.pos.y > canvas.height) {
			p.vel.y *= -1;
		}

		p.radius = radiusFactor * (1 + Math.sin(t * p.radiusFreq));
	}
	t++;
}

function draw() {
	step();
	ctx.strokeStyle = '#ffc';
	for (const p of points) {
		ctx.fillStyle = p.color;
		ctx.beginPath();
		ctx.arc(p.pos.x, p.pos.y, p.radius, 0, 2 * Math.PI);
		ctx.fill();

		ctx.beginPath();
		ctx.moveTo(p.pos.x, p.pos.y);
		ctx.lineTo(p.pos.x + 80 * p.vel.x, p.pos.y + 80 * p.vel.y);
		ctx.stroke();
	}

	requestAnimationFrame(draw);
}

function hsvToRgb(h, s, v) {
	const i = Math.floor(h * 6);
	const f = h * 6 - i;
	const p = v * (1 - s);
	const q = v * (1 - f * s);
	const t = v * (1 - (1 - f) * s);
	let r, g, b;
	switch (i % 6) {
		case 0:
			(r = v), (g = t), (b = p);
			break;
		case 1:
			(r = q), (g = v), (b = p);
			break;
		case 2:
			(r = p), (g = v), (b = t);
			break;
		case 3:
			(r = p), (g = q), (b = v);
			break;
		case 4:
			(r = t), (g = p), (b = v);
			break;
		case 5:
			(r = v), (g = p), (b = q);
			break;
	}

	function toHex(n) {
		let h = n.toString(16);
		if (h.length < 2) {
			h = `0${h}`;
		}
		return h;
	}

	return `#${toHex(Math.round(r * 255))}${toHex(Math.round(g * 255))}${toHex(
		Math.round(b * 255)
	)}`;
}