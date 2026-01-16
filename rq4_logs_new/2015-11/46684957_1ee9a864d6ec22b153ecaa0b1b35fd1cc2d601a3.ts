////////////////////////////////////////////////////////
//                                                             
//  Core2D.ts                                                  
//  ---------                                                  
//                                                             
//  Copyright Rik Mulder 2015
//       
////////////////////////////////////////////////////////

var gl;

class Shader {
    static PROJECTION_MATRIX: number = 100;
    static VIEW_MATRIX: number = 101;
    static MODEL_MATRIX: number = 102;
    static UV_MATRIX: number = 103;
    static COLOR: number = 104;

    programId: number = 0;

    vertices: number;
    UVCoords: number;

    shadVarData: number[] = Array(0);

    matrix: MatrixHandler;

    constructor(shaderVars: {}, vertex: string, fragment: string);
    constructor(shaderVars: {}, name: string);
    constructor(shaderVars: {}, p2: string, p3?: string) {
        var fragmentShader, vertexShader;
        if (typeof p3 == 'undefined') {
            fragmentShader = this.getShader(p2 + "-fs");
            vertexShader = this.getShader(p2 + "-vs");
        } else {
            fragmentShader = this.createShader(p3, gl.FRAGMENT_SHADER);
            vertexShader = this.createShader(p2, gl.VERTEX_SHADER);
        }

        this.programId = gl.createProgram();
        gl.attachShader(this.programId, vertexShader);
        gl.attachShader(this.programId, fragmentShader);
        gl.linkProgram(this.programId);

        if (!gl.getProgramParameter(this.programId, gl.LINK_STATUS)) {
            alert("Unable to initialize the shader program.");
        }
        this.bind();

        this.vertices = gl.getAttribLocation(this.programId, "vertexPos");
        this.UVCoords = gl.getAttribLocation(this.programId, "vertexUV");

        for (var key in shaderVars) {
            if (shaderVars.hasOwnProperty(key)) {
                this.shadVarData[shaderVars[key]] = gl.getUniformLocation(this.programId, key);
            }
        }

        this.matrix = new MatrixHandler(this);
    }

    bind() {
        gl.useProgram(this.programId);
    }

    setMatrix4(shadVar: number, matrix: number[]) {
        gl.uniformMatrix4fv(this.shadVarData[shadVar], false, matrix);
    }

    setInt(shadVar: number, num: number) {
        gl.uniform1i(this.shadVarData[shadVar], num);
    }

    setFloat(shadVar: number, num: number[]) {
        gl.uniformf(this.shadVarData[shadVar], num);
    }

    setVec2(shadVar: number, vec2: number[]) {
        gl.uniform2f(this.shadVarData[shadVar], vec2[0], vec2[1]);
    }

    setVec3(shadVar: number, vec3: number[]) {
        gl.uniform3f(this.shadVarData[shadVar], vec3[0], vec3[1], vec3[2]);
    }

    setVec4(shadVar: number, vec4: number[]) {
        gl.uniform4f(this.shadVarData[shadVar], vec4[0], vec4[1], vec4[2], vec4[3]);
    }

    createShader(data: string, shaderType) {
        var shader = gl.createShader(shaderType)
        gl.shaderSource(shader, data);
        gl.compileShader(shader);

        if (!gl.getShaderParameter(shader, gl.COMPILE_STATUS)) {
            alert("An error occurred compiling the shaders: " + gl.getShaderInfoLog(shader));
            return null;
        }
        return shader;
    }

    getShader(id: string) {
        var shaderScript, theSource, currentChild, shader;

        shaderScript = document.getElementById(id);

        if (!shaderScript) {
            return null;
        }

        theSource = "";
        currentChild = shaderScript.firstChild;

        while (currentChild) {
            if (currentChild.nodeType == currentChild.TEXT_NODE) {
                theSource += currentChild.textContent;
            }

            currentChild = currentChild.nextSibling;
        }

        if (shaderScript.type == "x-shader/x-fragment") {
            return this.createShader(theSource, gl.FRAGMENT_SHADER)
        } else if (shaderScript.type == "x-shader/x-vertex") {
            return this.createShader(theSource, gl.VERTEX_SHADER)
        } else {
            return null;
        }
    }
}

class MatrixHandler {
    shader: Shader;
    projMat = Matrix4.identity();
    viewMat = Matrix4.identity();

    constructor(shader: Shader) {
        this.shader = shader;
    }

    setProjectionMatrix(matrix) {
        this.shader.setMatrix4(Shader.PROJECTION_MATRIX, matrix)
        this.projMat = matrix;
    }

    setModelMatrix(matrix) {
        this.shader.setMatrix4(Shader.MODEL_MATRIX, matrix)
    }

    setViewMatrix(matrix) {
        this.shader.setMatrix4(Shader.VIEW_MATRIX, matrix)
        this.viewMat = matrix;
    }

    setUVMatrix(matrix) {
        this.shader.setMatrix4(Shader.UV_MATRIX, matrix)
    }
}

class Render {
    attrpBuffs: WebGLBuffer[] = new Array(0);
    attrpIds: WebGLBuffer[] = new Array(0);

    elementBuff: WebGLBuffer[] = new Array(0);
    count = new Array(0);

    shader: Shader;

    addAttrips(attripBuff: WebGLBuffer, id) {
        this.attrpBuffs.push(attripBuff);
        this.attrpIds.push(id);
    }
    addToEnd(elementBuff: WebGLBuffer, count: number) {
        this.elementBuff.push(elementBuff);
        this.count.push(count);
    }
    addVertexes(shader: Shader, vertices: number[]) {
        var buff = gl.createBuffer();
        var id = shader.vertices;
        this.shader = shader;
        gl.bindBuffer(gl.ARRAY_BUFFER, buff);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(vertices), gl.STATIC_DRAW);
        gl.vertexAttribPointer(id, 2, gl.FLOAT, false, 0, 0);

        this.addAttrips(buff, id);
    }
    addUVCoords(shader: Shader, coords: number[]) {
        var buff = gl.createBuffer();
        var id = shader.UVCoords;
        gl.bindBuffer(gl.ARRAY_BUFFER, buff);
        gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(coords), gl.STATIC_DRAW);
        gl.vertexAttribPointer(id, 2, gl.FLOAT, false, 0, 0);

        this.addAttrips(buff, id);
    }
    addIndieces(indieces: number[]) {
        var count = indieces.length;
        var elementBuff = gl.createBuffer();
        gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, elementBuff);
        gl.bufferData(gl.ELEMENT_ARRAY_BUFFER, new Uint16Array(indieces), gl.STATIC_DRAW);

        this.addToEnd(elementBuff, count);
    }
    switchBuff(id: number) {
        gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.elementBuff[id]);
    }
    draw(typ, count: number) {
        gl.drawArray(typ, 0, count);
    }
    drawElements(id: number, typ) {
        gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.elementBuff[id]);
        gl.drawElements(typ, this.count[id], gl.UNSIGNED_SHORT, 0);
    }
    drawSomeElements(ids: number[], typ) {
        for (var id = 0; id < ids.length; id++) {
            if (ids[id]) {
                gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.elementBuff[id]);
                gl.drawElements(typ, this.count[id], gl.UNSIGNED_SHORT, 0);
            }
        }
    }
    start() {
        for (var i = 0; i < this.attrpIds.length; i++) {
            gl.enableVertexAttribArray(this.attrpIds[i]);
            gl.bindBuffer(gl.ARRAY_BUFFER, this.attrpBuffs[i]);
            gl.vertexAttribPointer(this.attrpIds[i], 2, gl.FLOAT, false, 0, 0);
        }
    }
    end() {
        for (var i = 0; i < this.attrpIds.length; i++) {
            gl.disableVertexAttribArray(this.attrpIds[i]);
        }
    }
    drawElementsWithStartEnd(typ, id: number) {
        for (var i = 0; i < this.attrpIds.length; i++) {
            gl.enableVertexAttribArray(this.attrpIds[i]);
            gl.bindBuffer(gl.ARRAY_BUFFER, this.attrpBuffs[i]);
            gl.vertexAttribPointer(this.attrpIds[i], 2, gl.FLOAT, false, 0, 0);
        }

        gl.bindBuffer(gl.ELEMENT_ARRAY_BUFFER, this.elementBuff[id]);
        gl.drawElements(typ, this.count[id], gl.UNSIGNED_SHORT, 0);

        for (var i = 0; i < this.attrpIds.length; i++) {
            gl.disableVertexAttribArray(this.attrpIds[i]);
        }
    }

    drawWithStartEnd(typ, count: number) {
        for (var i = 0; i < this.attrpIds.length; i++) {
            gl.enableVertexAttribArray(this.attrpIds[i]);
            gl.bindBuffer(gl.ARRAY_BUFFER, this.attrpBuffs[i]);
            gl.vertexAttribPointer(this.attrpIds[i], 2, gl.FLOAT, false, 0, 0);
        }

        gl.drawArrays(typ, 0, count);

        for (var i = 0; i < this.attrpIds.length; i++) {
            gl.disableVertexAttribArray(this.attrpIds[i]);
        }
    }
}

class Framebuffer {
    frameBuffer;
    frameTexture;

    constructor(width: number, height: number) {
        this.frameTexture = gl.createTexture();
        gl.bindTexture(gl.TEXTURE_2D, this.frameTexture);
        gl.texImage2D(gl.TEXTURE_2D, 0, gl.RGBA, width, height, 0, gl.RGBA, gl.UNSIGNED_BYTE, null);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MAG_FILTER, gl.NEAREST);
        gl.texParameteri(gl.TEXTURE_2D, gl.TEXTURE_MIN_FILTER, gl.NEAREST_MIPMAP_NEAREST);

        this.frameBuffer = gl.createFramebuffer();
        gl.bindFramebuffer(gl.FRAMEBUFFER, this.frameBuffer);
        this.frameBuffer.width = width;
        this.frameBuffer.height = height;

        gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, this.frameTexture, 0);

        gl.bindTexture(gl.TEXTURE_2D, null);
        gl.bindFramebuffer(gl.FRAMEBUFFER, null);
    }

    startRenderTo() {
        gl.bindFramebuffer(gl.FRAMEBUFFER, this.frameBuffer);
        gl.clear(gl.COLOR_BUFFER_BIT);
        gl.viewport(0, 0, this.frameBuffer.width, this.frameBuffer.height);
    }

    stopRenderTo() {
        gl.bindTexture(gl.TEXTURE_2D, this.frameTexture);
        gl.generateMipmap(gl.TEXTURE_2D);
        gl.bindFramebuffer(gl.FRAMEBUFFER, null);
        gl.viewport(0, 0, window.innerWidth, window.innerHeight);
    }

    bindTexture() {
        gl.bindTexture(gl.TEXTURE_2D, this.frameTexture);
    }
}