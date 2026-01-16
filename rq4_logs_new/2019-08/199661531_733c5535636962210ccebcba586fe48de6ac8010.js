
function main() {
    // var gl = util.initWebgl('gl');
    // if(!gl) return

    var canvas = document.getElementById('gl');
    var gl = canvas.getContext('webgl');
    if (!gl) {
        console.log('webgl创建失败!');
        return
    }

    var points = new Float32Array([
        // 前面
        -0.5,0.5,0.5, 0.5,-0.5,0.5,  -0.5,-0.5,0.5,
        -0.5,0.5,0.5, 0.5,0.5,0.5,   0.5,-0.5,0.5,
        // 后面
        0.5,0.5,-0.5, -0.5,-0.5,-0.5, 0.5,-0.5,-0.5,
        0.5,0.5,-0.5, -0.5,0.5,-0.5, -0.5,-0.5,-0.5,
        //上面
        0.5,0.5,0.5,  -0.5,0.5,0.5,  -0.5,0.5,-0.5,
        0.5,0.5,0.5,  -0.5,0.5,-0.5, 0.5,0.5,-0.5,
        //下面
        -0.5,-0.5,0.5,  0.5,-0.5,0.5,  -0.5,-0.5,-0.5,
        0.5,-0.5,0.5,  0.5,-0.5,-0.5, -0.5,-0.5,-0.5,
        //左面
        -0.5,0.5,0.5,  -0.5,-0.5,0.5,  -0.5,0.5,-0.5,
        -0.5,-0.5,0.5,  -0.5,-0.5,-0.5, -0.5,0.5,-0.5,
        // 右面
        0.5,0.5,0.5,  0.5,0.5,-0.5, 0.5,-0.5,0.5,
        0.5,-0.5,0.5,  0.5,0.5,-0.5,  0.5,-0.5,-0.5,
    ])
    var directions = new Float32Array([
        0,0,1,  0,0,1,  0,0,1,  0,0,1,  0,0,1,  0,0,1,
        0,0,-1,  0,0,-1,  0,0,-1,  0,0,-1,  0,0,-1,  0,0,-1,
        0,1,0,  0,1,0,  0,1,0,  0,1,0,  0,1,0,  0,1,0,
        0,-1,0,  0,-1,0,  0,-1,0,  0,-1,0,  0,-1,0,  0,-1,0,
        -1,0,0,  -1,0,0,  -1,0,0,  -1,0,0,  -1,0,0,  -1,0,0,
        1,0,0,  1,0,0,  1,0,0,  1,0,0,  1,0,0,  1,0,0,
    ])
    var red = [200,0,0] ,green = [0,200,0], blue = [0,0,200]
    var colors = new Uint8Array([
        // 前面
        ...red,...red,...red,
        ...red,...red,...red,
        ...red,...red,...red,
        ...red,...red,...red,
        ...green,...green,...green,
        ...green,...green,...green,
        ...green,...green,...green,
        ...green,...green,...green,
        ...blue,...blue,...blue,
        ...blue,...blue,...blue,
        ...blue,...blue,...blue,
        ...blue,...blue,...blue,
    ])

    var program = util.createProgramFromScripts(gl,['vertex1','fragment1'])
    gl.useProgram(program)

    var modelMat = new Matrix4();
    var projectMat = new Matrix4();
    // 相机
    var fovy = 30,aspect=1,near=1,far = 100;
    // projectMat.setOrtho(-1,1,-1,1,-1,4)
    projectMat.setPerspective(fovy,aspect,near,far)
    projectMat.lookAt(3,3, 7, 0,0,0, 0,1,0);

    var u_project_loc = gl.getUniformLocation(program, 'u_project')
    gl.uniformMatrix4fv(u_project_loc, false, projectMat.elements)

    modelMat.setIdentity();
    var u_model_loc = gl.getUniformLocation(program,'u_model')
    gl.uniformMatrix4fv(u_model_loc,false,modelMat.elements)

    // 光线
    var vec3 = new Vector3([1,1,1]);
    var u_light = gl.getUniformLocation(program, 'u_dot_light_position')
    gl.uniform3fv(u_light,vec3.elements)

    var u_dot_light_color = gl.getUniformLocation(program, 'u_dot_light_color')
    gl.uniform3fv(u_dot_light_color,new Float32Array([1.0,1.0,1.0]))

    var u_environment_light_color = gl.getUniformLocation(program, 'u_environment_light_color')
    gl.uniform3fv(u_environment_light_color,new Float32Array([0.2,0.2,0.2]))

    var normal = new Matrix4().setIdentity()
    var u_nomal_loc = gl.getUniformLocation(program, 'u_normal');
    gl.uniformMatrix4fv(u_nomal_loc,false, normal.elements)

    bindArrayBuffer(points,program,'a_position',gl.FLOAT,false);
    bindArrayBuffer(colors,program,'a_color',gl.UNSIGNED_BYTE,true);
    bindArrayBuffer(directions,program,'a_direction',gl.FLOAT,false);

    gl.enable(gl.DEPTH_TEST)
    gl.clearColor(0, 0, 0, 1)
    gl.clear(gl.COLOR_BUFFER_BIT)

    gl.drawArrays(gl.TRIANGLES, 0, 36)

    function bindArrayBuffer(data,program,attribute,type,normal) {
        var buffer = gl.createBuffer();
        gl.bindBuffer(gl.ARRAY_BUFFER,buffer)
        gl.bufferData(gl.ARRAY_BUFFER, data, gl.STATIC_DRAW)
        var a_loc = gl.getAttribLocation(program,attribute)
        gl.enableVertexAttribArray(a_loc);
        var size = 3;          // 每次迭代运行提取两个单位数据
        var dataType = type || gl.FLOAT;   // 每个单位的数据类型是32位浮点型
        var normalize = normal; // 不需要归一化数据
        var stride = 0;        // 0 = 移动单位数量 * 每个单位占用内存（sizeof(type)）
        var offset = 0;        // 从缓冲起始位置开始读取
        gl.vertexAttribPointer(a_loc, size, dataType, normalize, stride, offset)
    }

    var model = new Matrix4();
    model.setIdentity();
    function renderView(degX,degY) {
        var normal = new Matrix4();
        model.rotate(degY,0,1,0).rotate(degX,1,0,0);
        normal.setInverseOf(model);
        normal.transpose();
        gl.uniformMatrix4fv(u_model_loc, false, model.elements);
        gl.uniformMatrix4fv(u_nomal_loc, false, normal.elements);
        gl.clear(gl.COLOR_BUFFER_BIT | gl.DEPTH_BUFFER_BIT)
        gl.drawArrays(gl.TRIANGLES, 0, 36)
    }

    function bind() {
        var isMove = false;
        var canvas = document.getElementById('gl');
        canvas.addEventListener('mousedown',function(e) {
            canvas.style.cursor = 'grabbing';
            isMove = true;
        })
        canvas.addEventListener('mouseup',function(e) {
            canvas.style.cursor = 'grab';
            isMove = false;
        })
        canvas.addEventListener('mouseleave',function(e) {
            canvas.style.cursor = 'grab';
            isMove = false;
        })

        canvas.addEventListener('mousemove',function(e) {
            if(isMove) {
                console.log(e);
                var x = e.movementX;
                var y = e.movementY;
                var degY = x / 500 * 360;
                var degX = y / 500 * 360;
                renderView(degX,degY)
            }
        })

    }

    bind();
}

main();