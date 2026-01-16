//Matthew Seymour, matthew dot seymour at gmail.com

"use strict";

var Compute = {};

Compute.FFT_DIRECTIONS = {FORWARD: 1, BACKWARD: 2};


Compute.getPrograms = function(gl) {
    var computeProgs = {};
    
    computeProgs.gl = gl;
    
    computeProgs.unitRectBuffer = glUtils.generateSimpleUnitRectangleBuffer(gl);
    
    

    
    computeProgs.transformProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, transposeFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image", "u_transpose"]
    );

    computeProgs.scaleProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, scaleFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image", "u_scale"]
    );

    computeProgs.bitReverseProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, bitReverseFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image", "u_bitReverse", "u_n"]
    );

    computeProgs.stageProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, stageFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image", "u_twiddleRead", "u_numStages", "u_n", "u_stage", "u_reduce"]
    );

    computeProgs.colorMapProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, colorMapFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image_0", "u_image_1", "u_image_2", "u_colorMap"]
    );

    computeProgs.bicubicProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, bicubicFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image", "u_size"]
    );

    computeProgs.gradientProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, gradientFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_image", "u_size", "u_gradVec", "u_dist"]
    );
    
    computeProgs.noiseProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, noiseFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_seed"]
    );
    
    computeProgs.gaussianNoiseProgramInfo = glUtils.makeProgram(
        gl, computeVertexSource, gaussianNoiseFragmentSource, 
        ["a_position", "a_texCoord"], 
        ["u_seed", "u_scale"]
    );
    
    
    
    return computeProgs;
}

Compute.makeComputeBuffer = function(gl, width, height) {
    var textureInfo = glUtils.makeTextureGeneric(gl, width, height, gl.FLOAT, gl.NEAREST, gl.CLAMP_TO_EDGE, null);

    var framebuffer = gl.createFramebuffer();
    gl.bindFramebuffer(gl.FRAMEBUFFER, framebuffer);

    gl.framebufferTexture2D(gl.FRAMEBUFFER, gl.COLOR_ATTACHMENT0, gl.TEXTURE_2D, textureInfo.texture, 0);
    
    return {width: width, height: height, texture: textureInfo.texture, frameBuffer: framebuffer};
}

Compute.buildCustomProgram = function(gl, code) {
    var fragmentSource = transformFragmentSource1 + code + transformFragmentSource2;
    var programInfo = glUtils.makeProgram(
        gl, computeVertexSource, fragmentSource,
        ["a_position", "a_texCoord"],
        ["u_image","u_1","u_2","u_3","u_4"]
    );
    programInfo.unitRectBuffer = glUtils.generateSimpleUnitRectangleBuffer(gl);
    return programInfo;
}

Compute.buildCustomProgram2 = function(gl, code) {
    var fragmentSource = transform2FragmentSource1 + code + transform2FragmentSource2;
    var programInfo = glUtils.makeProgram(
        gl, computeVertexSource, fragmentSource,
        ["a_position", "a_texCoord"],
        ["u_image0", "u_image1","u_1","u_2","u_3","u_4"]
    );
    programInfo.unitRectBuffer = glUtils.generateSimpleUnitRectangleBuffer(gl);
    return programInfo;
}

Compute.buildReduceProgram = function(gl, code) {
    var fragmentSource = reduceFragmentSource1 + code + reduceFragmentSource2;
    var programInfo = glUtils.makeProgram(
        gl, computeVertexSource, fragmentSource,
        ["a_position", "a_texCoord"],
        ["u_image","u_offset","u_1","u_2","u_3","u_4"]
    );
    programInfo.unitRectBuffer = glUtils.generateSimpleUnitRectangleBuffer(gl);
    return programInfo;
}



Compute.makePlan = function(computeProgs, stages, direction, reduce) {
    var N = 1 << stages;
    
    var reduceFactor = Math.pow(reduce, 1 / stages);
    
    function getIndex(x, y, width) {
        return (x + y * width) * 4;
    }
    
    //Bit reverse N x 1
    var imageDataBitReverse = new Float32Array(N * 1 * 4);
    for(var i = 0; i < N; i++) {
        //Bit reversal:
        var k1 = 1; 
        var k2 = N >> 1;
        var j = 0; //output index
        while(k1 < N) {
            j += (Math.floor(i / k2) % 2) * k1;
            k1 = k1 << 1;
            k2 = k2 >> 1;
        }
    
        imageDataBitReverse[i * 4    ] = j;
        imageDataBitReverse[i * 4 + 1] = 0;
        imageDataBitReverse[i * 4 + 2] = 0;
        imageDataBitReverse[i * 4 + 3] = 0;
    }

    var imageDataTwiddleRead = new Float32Array(N * stages * 4);

    
    var outputSize = 2;
    var row = 0;
    var dirFactor = (direction == Compute.FFT_DIRECTIONS.FORWARD ? 1 : -1);
    while(outputSize <= N) {
        for(var n = 0; n < N / outputSize; n++) {
            for(var k = 0; k < outputSize / 2; k++) {
                var i1 = outputSize * n + k;
                var i2 = i1 + outputSize / 2;
                
                var twiddle_r = Math.cos(- dirFactor * 2 * Math.PI * k / outputSize);
                var twiddle_i = Math.sin(- dirFactor * 2 * Math.PI * k / outputSize);
                
                //Even:
                imageDataTwiddleRead[getIndex(i1, row, N) + 0] = twiddle_r;
                imageDataTwiddleRead[getIndex(i1, row, N) + 1] = twiddle_i;
                //Odd:
                imageDataTwiddleRead[getIndex(i1, row, N) + 2] = i1;
                imageDataTwiddleRead[getIndex(i1, row, N) + 3] = i2;

            
                //Even
                imageDataTwiddleRead[getIndex(i2, row, N) + 0] = -twiddle_r;
                imageDataTwiddleRead[getIndex(i2, row, N) + 1] = -twiddle_i;
                //Odd
                imageDataTwiddleRead[getIndex(i2, row, N) + 2] = i1;
                imageDataTwiddleRead[getIndex(i2, row, N) + 3] = i2;
                
            }
        }
        outputSize *= 2;
        row++;
    }

    return {
        N           : N, 
        reduce      : reduceFactor,
        stages      : stages, 
        bitReverse  : glUtils.makeTextureGeneric(gl, N, 1     , gl.FLOAT, gl.NEAREST, gl.CLAMP_TO_EDGE, imageDataBitReverse),
        twiddleRead : glUtils.makeTextureGeneric(gl, N, stages, gl.FLOAT, gl.NEAREST, gl.CLAMP_TO_EDGE, imageDataTwiddleRead      ),        
    }
        
    
}

Compute.computeFft = function(computeProgs, fftPlan, data, outputFrameBuffer, tempFrameBuffer) {
    var gl = computeProgs.gl;
    var stages = fftPlan.stages;
    var size = fftPlan.N;
    
    //Calc steps:
    var directions = {FORWARD: 0, BACKWARD: 1};
    

    function bitReverse(inputBuffer, outputBuffer) {
        //Bit reverse step:
    	gl.useProgram(computeProgs.bitReverseProgramInfo.program);
    
        gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
        gl.viewport(0, 0, size, size); 
    
        glUtils.bindRectBuffer(gl, computeProgs.bitReverseProgramInfo, computeProgs.unitRectBuffer);

        gl.activeTexture(gl.TEXTURE0);
    	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);
    
        gl.activeTexture(gl.TEXTURE1);
    	gl.bindTexture(gl.TEXTURE_2D, fftPlan.bitReverse.texture);

    	computeProgs.bitReverseProgramInfo.setters.u_image(0);
    	computeProgs.bitReverseProgramInfo.setters.u_bitReverse(1);
    	computeProgs.bitReverseProgramInfo.setters.u_n(size);
    
        gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    }
    

    function getFromBuffer(direction) {
        if(direction == directions.FORWARD) {
            return outputFrameBuffer;
        } else if(direction == directions.BACKWARD) {
            return tempFrameBuffer;
        }
    }
    
    function getToBuffer(direction) {
        if(direction == directions.FORWARD) {
            return tempFrameBuffer;
        } else if(direction == directions.BACKWARD) {
            return outputFrameBuffer;
        }
    }

    function swapDirection(direction) {
        if(direction == directions.FORWARD) {
            return directions.BACKWARD;
        } else if(direction == directions.BACKWARD) {
            return directions.FORWARD;
        }
    }
    
    
    function transform(direction) {
    	gl.useProgram(computeProgs.stageProgramInfo.program);
    
        glUtils.bindRectBuffer(gl, computeProgs.stageProgramInfo, computeProgs.unitRectBuffer);
    
        gl.activeTexture(gl.TEXTURE1);
    	gl.bindTexture(gl.TEXTURE_2D, fftPlan.twiddleRead.texture);
        
    	computeProgs.stageProgramInfo.setters.u_image(0);
    	computeProgs.stageProgramInfo.setters.u_twiddleRead(1);
    	computeProgs.stageProgramInfo.setters.u_reduce(fftPlan.reduce);
    	computeProgs.stageProgramInfo.setters.u_numStages(stages);
    	computeProgs.stageProgramInfo.setters.u_n(size);
        
        for(var i = 0; i < stages; i++) {
            gl.activeTexture(gl.TEXTURE0);
        	gl.bindTexture(gl.TEXTURE_2D, getFromBuffer(direction).texture);
            
        	computeProgs.stageProgramInfo.setters.u_stage(i);

            gl.bindFramebuffer(gl.FRAMEBUFFER, getToBuffer(direction).frameBuffer);
        	
            gl.viewport(0, 0, size, size); 
        
	    	gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
            
            gl.flush();
            
            direction = swapDirection(direction);
        }
        return direction;
    }

    
    function transpose(direction, straightCopy) {
        var bufferIn = getFromBuffer(direction);
        var bufferOut = getToBuffer(direction);

    	gl.useProgram(computeProgs.transformProgramInfo.program);
    
        gl.bindFramebuffer(gl.FRAMEBUFFER, bufferOut.frameBuffer);
        gl.viewport(0, 0, size, size); 
    
        glUtils.bindRectBuffer(gl, computeProgs.transformProgramInfo, computeProgs.unitRectBuffer);

        gl.activeTexture(gl.TEXTURE0);
    	gl.bindTexture(gl.TEXTURE_2D, bufferIn.texture);
    
        computeProgs.transformProgramInfo.setters.u_image(0);
        computeProgs.transformProgramInfo.setters.u_transpose(straightCopy ? 0 : 1);
    
        gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);

        return swapDirection(direction);
    }

    bitReverse(data, tempFrameBuffer);
    var dir = directions.BACKWARD;

    
    dir = transform(dir);
    dir = transpose(dir, false);
    
    bitReverse(getFromBuffer(dir), getToBuffer(dir));
    dir = swapDirection(dir);

    dir = transform(dir);
    dir = transpose(dir, false);

    if(dir == directions.BACKWARD)
        transpose(dir, true);
}

Compute.colorMap = function(computeProgs, colorMap, inputBuffer, outputBuffer) {
    var gl = computeProgs.gl;
	
    gl.useProgram(computeProgs.colorMapProgramInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, computeProgs.colorMapProgramInfo, computeProgs.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);

    gl.activeTexture(gl.TEXTURE1);
	gl.bindTexture(gl.TEXTURE_2D, colorMap.texture);

    computeProgs.colorMapProgramInfo.setters.u_image(0);
    computeProgs.colorMapProgramInfo.setters.u_colorMap(1);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

Compute.scale = function(computeProgs, scaleFactor, inputBuffer, outputBuffer) {
    var gl = computeProgs.gl;
	
    gl.useProgram(computeProgs.scaleProgramInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, computeProgs.scaleProgramInfo, computeProgs.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);

    computeProgs.scaleProgramInfo.setters.u_image(0);
    computeProgs.scaleProgramInfo.setters.u_scale(scaleFactor);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

Compute.bicubic = function(computeProgs, inputBuffer, outputBuffer) {
    var gl = computeProgs.gl;
	
    gl.useProgram(computeProgs.bicubicProgramInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, computeProgs.bicubicProgramInfo, computeProgs.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);

    computeProgs.bicubicProgramInfo.setters.u_image(0);
    computeProgs.bicubicProgramInfo.setters.u_size([inputBuffer.width, inputBuffer.height]);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

Compute.gradient = function(computeProgs, directionVector, dist, inputBuffer, outputBuffer) {
    var gl = computeProgs.gl;
	
    gl.useProgram(computeProgs.gradientProgramInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, computeProgs.gradientProgramInfo, computeProgs.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);

    computeProgs.gradientProgramInfo.setters.u_image(0);
    computeProgs.gradientProgramInfo.setters.u_size([inputBuffer.width, inputBuffer.height]);
    computeProgs.gradientProgramInfo.setters.u_gradVec(directionVector);
    computeProgs.gradientProgramInfo.setters.u_dist(dist);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

Compute.noise = function(computeProgs, seed, outputBuffer) {
    var gl = computeProgs.gl;
	
    gl.useProgram(computeProgs.noiseProgramInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, computeProgs.noiseProgramInfo, computeProgs.unitRectBuffer);
    
    computeProgs.noiseProgramInfo.setters.u_seed(seed);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

Compute.gaussianNoise = function(computeProgs, seed, scale, outputBuffer) {
    var gl = computeProgs.gl;
	
    gl.useProgram(computeProgs.gaussianNoiseProgramInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, computeProgs.gaussianNoiseProgramInfo, computeProgs.unitRectBuffer);
    
    computeProgs.gaussianNoiseProgramInfo.setters.u_seed(seed);
    computeProgs.gaussianNoiseProgramInfo.setters.u_scale(scale);

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}


Compute.runCustomProgram = function(gl, programInfo, inputBuffer, outputBuffer, u_1, u_2, u_3, u_4) {
	gl.useProgram(programInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, programInfo, programInfo.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);

    programInfo.setters.u_image(0);
    
    if(u_1 !== undefined)
        programInfo.setters.u_1(u_1);
    
    if(u_2 !== undefined)
        programInfo.setters.u_2(u_2);
    
    if(u_3 !== undefined)
        programInfo.setters.u_3(u_3);

    if(u_4 !== undefined)
        programInfo.setters.u_4(u_4);
    
        

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}

Compute.runReduceProgram = function(gl, programInfo, inputBuffer, outputBuffer, tempBuffer, u_1, u_2, u_3, u_4) {
    /*
	gl.useProgram(programInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, programInfo, programInfo.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer.texture);

    programInfo.setters.u_image(0);
    
    if(u_1 !== undefined)
        programInfo.setters.u_1(u_1);
    
    if(u_2 !== undefined)
        programInfo.setters.u_2(u_2);
    
    if(u_3 !== undefined)
        programInfo.setters.u_3(u_3);

    if(u_4 !== undefined)
        programInfo.setters.u_4(u_4);
    
        

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
    */
}

Compute.runCustomProgram2 = function(gl, programInfo, inputBuffer0, inputBuffer1, outputBuffer, u_1, u_2, u_3, u_4) {
	gl.useProgram(programInfo.program);

    gl.bindFramebuffer(gl.FRAMEBUFFER, outputBuffer.frameBuffer);
    gl.viewport(0, 0, outputBuffer.width, outputBuffer.height); 

    glUtils.bindRectBuffer(gl, programInfo, programInfo.unitRectBuffer);

    gl.activeTexture(gl.TEXTURE0);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer0.texture);
    
    gl.activeTexture(gl.TEXTURE1);
	gl.bindTexture(gl.TEXTURE_2D, inputBuffer1.texture);

    programInfo.setters.u_image0(0);
    programInfo.setters.u_image1(1);

    if(u_1 !== undefined)
        programInfo.setters.u_1(u_1);
    
    if(u_2 !== undefined)
        programInfo.setters.u_2(u_2);
    
    if(u_3 !== undefined)
        programInfo.setters.u_3(u_3);
    
    if(u_4 !== undefined)
        programInfo.setters.u_3(u_4);
    

    gl.drawArrays(gl.TRIANGLE_STRIP, 0, 4);
}