///<reference path="Plane.ts" />
///<reference path="Shader.ts" />
class Scene {
	
	numberOfTriangles: number;
	data: Float32Array;
	vertexCount: number = 0;
	shader: Shader;
	mvp: Float32Array;
	gl: WebGLRenderingContext;
	bufferid: WebGLBuffer;
	texture: WebGLTexture;

	constructor(gl, shader, texture,numberOfTriangles) {
		this.numberOfTriangles = numberOfTriangles;
		this.data = new Float32Array(this.numberOfTriangles);
		this.shader = shader;
		this.gl = gl;
		this.bufferid = this.gl.createBuffer();
		this.texture = texture;
	}

	addPlane(plane:Plane){
		var j = this.vertexCount*9;
		for (var i = 0; i < plane.v.length; i++){
			var vertex = plane.v[i];
			this.data[j+0] = vertex.x
			this.data[j+1] = vertex.y
			this.data[j+2] = vertex.z
			this.data[j+3] = vertex.r
			this.data[j+4] = vertex.g
			this.data[j+5] = vertex.b
			this.data[j+6] = vertex.a
			this.data[j+7] = vertex.u
			this.data[j+8] = vertex.v
			j += 9;
		}
		this.vertexCount += plane.v.length;
	}

	render(){
		this.gl.bindBuffer(this.gl.ARRAY_BUFFER, this.bufferid);
		this.gl.bufferData(this.gl.ARRAY_BUFFER, this.data, this.gl.STATIC_DRAW);

		var p = this.shader.program;
		this.gl.useProgram(p);

		var mvp_matrix = this.gl.getUniformLocation(p, "u_mvp_matrix");
		this.gl.uniformMatrix4fv(mvp_matrix, false, this.mvp);

		this.gl.activeTexture(this.gl.TEXTURE0);
  		this.gl.bindTexture(this.gl.TEXTURE_2D, this.texture);

  		var u_sampler = this.gl.getUniformLocation(p, "u_sampler");
		this.gl.uniform1i(u_sampler, 0);

		var a_vertex = this.gl.getAttribLocation(p, "a_vertex");
		this.gl.enableVertexAttribArray(a_vertex);
		this.gl.vertexAttribPointer(a_vertex, 3, this.gl.FLOAT, false, 9*4, 0);

		var a_color = this.gl.getAttribLocation(p, "a_color");
		this.gl.enableVertexAttribArray(a_color);
		this.gl.vertexAttribPointer(a_color, 4, this.gl.FLOAT, false, 9*4, 3*4);

		var a_texcoord = this.gl.getAttribLocation(p, "a_texcoord");
		this.gl.enableVertexAttribArray(a_texcoord);
		this.gl.vertexAttribPointer(a_texcoord, 2, this.gl.FLOAT, false, 9*4, 7*4);

		this.gl.drawArrays(this.gl.TRIANGLES, 0, this.vertexCount);

		this.vertexCount = 0;
	}
}