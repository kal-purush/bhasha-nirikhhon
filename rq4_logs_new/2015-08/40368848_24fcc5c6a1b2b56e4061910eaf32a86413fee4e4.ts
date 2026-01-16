/// <reference path='view.ts'/>
/// <reference path='./node_modules/js2glsl/js2glsl.d.ts'/>

import js2glsl = require("js2glsl") 
import IViewEntity = require("ViewEntity");
import View = require("View");

function getExpectedGlSize(gl : WebGLRenderingContext, info : WebGLActiveInfo) {
    switch(info.type) {
    case gl.INT:         
    case gl.FLOAT:       return 1;
    case gl.INT_VEC2:    
    case gl.FLOAT_VEC2:  return 2;
    case gl.INT_VEC3:    
    case gl.FLOAT_VEC3:  return 3;
    case gl.INT_VEC4:    
    case gl.FLOAT_VEC4:  return 4;
    }    
    throw new Error("Unexpected type");
}


interface Hashtable<V> {
    [key: string] : V; 
};

class GlEntity<T,U,V> implements IViewEntity {
    buffers : Hashtable<[WebGLBuffer,number]>; 

    constructor(public shaderSpec : js2glsl.ShaderSpecification<T,V,U>, public uniforms : U = null ) {                
        this.buffers = {}; 
    }

    getUniformBindFunction(gl : WebGLRenderingContext, info : WebGLActiveInfo) {	
	return gl.uniform2f; 
    }

    getUniformSetFunction(gl : WebGLRenderingContext, info : WebGLActiveInfo) {
	function getFnSuffix(t : number) {
	    switch(t) {
	    case gl.FLOAT:       return "1f";
	    case gl.FLOAT_VEC2:  return "2f";
	    case gl.FLOAT_VEC3:  return "3f";
	    case gl.FLOAT_VEC4:  return "4f";
	    case gl.FLOAT_MAT2:  return "Matrix2fv";
	    case gl.FLOAT_MAT3:  return "Matrix3fv";
	    case gl.FLOAT_MAT4:  return "Matrix4fv";
	    case gl.INT:         return "1i";
	    case gl.INT_VEC2:    return "2i";
	    case gl.INT_VEC3:    return "3i";
	    case gl.INT_VEC4:    return "4i";
	    }
	    throw new Error("Unrecognized uniform type: " + t); 
	};

	var isArray = info.size > 1; 
	var suffix = getFnSuffix(info.type);
	var name = "uniform" + suffix + (isArray ? "v" : ""); 	
	var isMat = suffix[0] == 'M'; 
	
	if(isMat)
	    return function(gl : WebGLRenderingContext, l:number, v:[number]) { 
		var fn = <(location: WebGLUniformLocation, f:number, v: number[]) => void> (<any>gl)[name]; 
		fn(l, /*gl.FALSE*/0, v); 
	    }
	if(isArray)
	    return function(gl : WebGLRenderingContext, l:number, v:[number]) { 
		var fn = <(location: WebGLUniformLocation, v: number[]) => void> (<any>gl)[name]; 
		fn(l, v); 
	    }
	return function(gl : WebGLRenderingContext,  l:number, v:[number]) { 
	    var fn = <(location: WebGLUniformLocation, v: number[]) => void> (<any>gl)[name]; 
	    fn.apply(gl, [l].concat(v) ); 
	}
	
    }

    setUniform(gl : WebGLRenderingContext, program : WebGLProgram, info : WebGLActiveInfo) {
	var location = gl.getUniformLocation(program, info.name);
	var v = (<any>this.uniforms)[info.name]; 
	var isArray = info.size > 1; 
	var name = "uniform";
	
	var fn : any = this.getUniformSetFunction(gl, info);
	fn(gl, location, v);
    }
    render(view : View) {
        var gl = view.gl;
        var program = this.getProgram(gl);
        gl.useProgram(program);        

        var numAttribs = gl.getProgramParameter(program, gl.ACTIVE_ATTRIBUTES);        
        for (var i = 0; i < numAttribs; ++i) {
	    var info = gl.getActiveAttrib(program, i);
	    var buffer = this.getBuffer(gl, info.name); 
	    gl.bindBuffer(gl.ARRAY_BUFFER, buffer[0]); 

	    var location = gl.getAttribLocation(program, info.name);
	    gl.enableVertexAttribArray(location);
	    gl.vertexAttribPointer(location, buffer[1], gl.FLOAT, false, 0, 0);
        }

	var numUniforms = gl.getProgramParameter(program, gl.ACTIVE_UNIFORMS);        
	for (var i = 0;i < numUniforms;i++) {
	    var info = gl.getActiveUniform(program, i); 
	    this.setUniform(gl, program, info); 
	}

    }    
    getProgram(gl : WebGLRenderingContext) {
        return this.shaderSpec.GetProgram(gl); 
    }
    getBufferData(n : string) : [Float32Array, number] {
        return [new Float32Array(0), 1];
    }
    getBuffer(gl : WebGLRenderingContext, n : string) : [WebGLBuffer, number] {
        if(this.buffers[n])
	    return this.buffers[n];
        this.buffers[n] = [gl.createBuffer(), 0];
        
        gl.bindBuffer(gl.ARRAY_BUFFER, this.buffers[n][0]);        
	var bufferData = this.getBufferData(n); 
        gl.bufferData(gl.ARRAY_BUFFER, bufferData[0], gl.STATIC_DRAW);
	this.buffers[n][1] = bufferData[1];
        return this.buffers[n];
    }
}

class TupleListEntity <T, U, V, TT> extends GlEntity<T,U,V> {
    constructor(public data : TT[], 
		public tupleSize : number, 
		shaderSpec : js2glsl.ShaderSpecification<T,V,U>, 
		public glMode : string, 
		u : U) {
        super(shaderSpec, u);        
    }    

    getBufferData(n : string) : [Float32Array, number] {
        if(this.data.length == 0)
	    return [new Float32Array(0), 1];
	
	var data = <any>this.data;
        var elemSize = data[0][0][n].length;
        if(elemSize === undefined) 
	    elemSize = 1;
        
	var currIdx = 0;

        var rtn = new Float32Array(this.data.length * elemSize * this.tupleSize);        
        for(var i = 0;i < this.data.length;i++){
	    for(var idx = 0;idx < this.tupleSize;idx++)
		for(var j = 0;j < elemSize;j++)
                    rtn[currIdx++] = elemSize == 1 ? data[i][idx][n] : data[i][idx][n][j];
        }
	console.log(this,rtn);
        return [rtn, elemSize];
    }

    render(view : View) {         
        super.render(view);
        var gl = view.gl;
	var mode = (<any>gl)[this.glMode];
        gl.drawArrays(mode, 0, this.data.length * this.tupleSize);
    }    

}

class ListEntity <T, U, V> extends GlEntity<T,U,V> {    
   constructor(public data : T[], 
	       shaderSpec : js2glsl.ShaderSpecification<T,V,U>, 
	       public glMode : string,
	       u : U) {
        super(shaderSpec, u);        
    }    
    getBufferData(n : string) : [Float32Array, number] {
        if(this.data.length == 0)
	    return [new Float32Array(0), 1];

	var data = <any> this.data;
        var elemSize = data[0][n].length;
        if(elemSize === undefined) 
	    elemSize = 1;
        
	var currIdx = 0; 
        var rtn = new Float32Array(data.length * elemSize);        
        for(var i = 0;i < data.length;i++){
		for(var j = 0;j < elemSize;j++)
                    rtn[currIdx++] = elemSize == 1 ? data[i][n] : data[i][n][j];
        }
        return [rtn, elemSize];
    }
    render(view : View) {         
        super.render(view);
        var gl = view.gl;
	var mode = (<any>gl)[this.glMode];
        gl.drawArrays(mode, 0, this.data.length);
    }    
}


export class TrianglesEntity <T,U,V> extends TupleListEntity<T, U, V, [T, T, T]> {
    constructor(public data : [T,T,T] [], 
		shaderSpec : js2glsl.ShaderSpecification<T,V,U>, 
		u : U = null) {
        super(data, 3, shaderSpec, "TRIANGLES", u);        
    }
}

export class TriangleStripEntity <T,U,V> extends ListEntity<T,U,V> {    
    constructor(public data : T[], shaderSpec : js2glsl.ShaderSpecification<T,V,U>, u : U) {
        super(data, shaderSpec, "TRIANGLE_STRIP", u);        
	if(data.length < 3) {
	    throw new Error("TriangleStrip requires 3 + n number of data points."); 
	}
    }
}

export class TriangleFanEntity <T,U,V> extends ListEntity<T,U,V> {    
    constructor(public data : T[], shaderSpec : js2glsl.ShaderSpecification<T,V,U>, u : U) {
        super(data, shaderSpec, "TRIANGLE_FAN", u);        
	if(data.length < 3) {
	    throw new Error("TriangleFan requires 3 + n number of data points."); 
	}
    }
}

export class LinesEntity <T,U,V> extends TupleListEntity<T,U,V, [T,T]> {
    constructor(public data : [T,T] [], shaderSpec : js2glsl.ShaderSpecification<T,V,U>, u : U = null) {
        super(data, 2, shaderSpec, "LINES", u);        
    }
};

export class LineLoopEntity <T,U,V> extends ListEntity<T,U,V> {
   constructor(public data : T[], shaderSpec : js2glsl.ShaderSpecification<T,V,U>, u : U = null) {
        super(data, shaderSpec, "LINE_LOOP", u);        
    }
}

export class LineStripEntity <T,U,V> extends ListEntity<T,U,V> {    
    constructor(public data : T[], shaderSpec  : js2glsl.ShaderSpecification<T,V,U>, u : U = null) {
        super(data, shaderSpec, "LINE_STRIP", u);        
	if(data.length < 2) {
	    throw new Error("LineStrips require 2 + n number of data points."); 
	}
    }
}

export class PointsEntity <T,U,V> extends ListEntity<T,U,V> {
    constructor(public data : T[], shaderSpec : js2glsl.ShaderSpecification<T,V,U>, u : U = null) {
	super(data, shaderSpec, "POINTS", u);        
    }
}