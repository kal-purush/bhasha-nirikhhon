///<reference path="Shader.ts" />
///<reference path="Color.ts" />
///<reference path="Util.ts" />
class Game {

	canvas: HTMLCanvasElement;
	gl: WebGLRenderingContext;

	constructor() {
		this.canvas = <HTMLCanvasElement>document.getElementById("stage");
		this.gl = <WebGLRenderingContext>this.canvas.getContext("webgl");
	}

	init(){
		var clearColor = new Color(0xFFFF00FF);
		var shader = new Shader(this.gl, document.getElementById("vs").innerText, document.getElementById("fs").innerText);

		var mvp = createOrthographicMatrix(0, 640, 0, 480, -1, -1000);

		this.gl.clearColor(clearColor.r, clearColor.g, clearColor.b, clearColor.a);
		this.gl.clear(this.gl.COLOR_BUFFER_BIT);
	}
}