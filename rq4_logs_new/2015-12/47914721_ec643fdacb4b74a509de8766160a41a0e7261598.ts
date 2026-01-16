///<reference path="Color.ts" />
///<reference path="UV.ts" />
///<reference path="Vertex.ts" />
///<reference path="UV.ts" />

class Plane {

	cx: number;
	cy: number;
	z: number;
	hw: number;
	hh: number;
	color: Color;
	uv: UV;
	//TODO may implement if use
	rotation: number = 0;

	v: Array<Vertex> = new Array(6);

	constructor(cx: number, cy: number, z: number, hw: number, hh: number, color:Color, uv:UV) {
		this.cx = cx;
		this.cy = cy;
		this.z = z;
		this.hw = hw;
		this.hh = hh;
		this.color = color;
		this.uv = uv;

		this.v[0] = new Vertex(cx-hw, cy-hh, z, color.r, color.g, color.b, color.a, uv.u, uv.v);
		this.v[1] = new Vertex(cx-hw, cy+hh, z, color.r, color.g, color.b, color.a, uv.u, uv.v2);
		this.v[2] = new Vertex(cx+hw, cy-hh, z, color.r, color.g, color.b, color.a, uv.u2, uv.v);
		this.v[3] = new Vertex(cx+hw, cy-hh, z, color.r, color.g, color.b, color.a, uv.u2, uv.v);
		this.v[4] = new Vertex(cx+hw, cy+hh, z, color.r, color.g, color.b, color.a, uv.u2, uv.u2);
		this.v[5] = new Vertex(cx-hw, cy+hh, z, color.r, color.g, color.b, color.a, uv.u, uv.u2);
	}
}