class Shader {
	
	gl: WebGLRenderingContext;
	program: WebGLProgram;

	constructor(gl, vertexShaderSource, fragmentShaderSource) {
		this.gl = gl;
		this.program = gl.createProgram();

		var vertexShader = gl.createShader(gl.VERTEX_SHADER);
		gl.shaderSource(vertexShader, vertexShaderSource);
		gl.compileShader(vertexShader);
		if (!gl.getShaderParameter(vertexShader, gl.COMPILE_STATUS)) {
			console.log(gl.getShaderInfoLog(vertexShader));
		}

		var fragmentShader = gl.createShader(gl.FRAGMENT_SHADER);
		gl.shaderSource(fragmentShader, fragmentShaderSource);
		gl.compileShader(fragmentShader);
		if (!gl.getShaderParameter(fragmentShader, gl.COMPILE_STATUS)) {
			console.log(gl.getShaderInfoLog(fragmentShader));
		}

		gl.attachShader(this.program, vertexShader);
		gl.attachShader(this.program, fragmentShader);

		gl.linkProgram(this.program);

		if (!gl.getProgramParameter(this.program, gl.LINK_STATUS)){
			var lastError = gl.getProgramInfoLog(this.program);
			console.warn("Error in program linking:" + lastError);
			gl.deleteProgram(this.program);
		}
	}
}