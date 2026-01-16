import * as math from './math.js';

export default class {
  /**
   * @param {HTMLCanvasElement} canvas Canvas to draw to
   */
  constructor(canvas) {
    this.canvas = canvas;
  }

  async run() {
    await this.load();
    this.initGL();

    this.matrix = mat3.create();
    mat3.fromScaling(this.matrix, [2, 2]);

    this.resize();
    addEventListener('resize', () => this.resize());

    document.addEventListener(
      'mousewheel',
      e => {
        const scaleFactor = Math.pow(10, e.wheelDeltaY / 1000);
        const mouseX =
          (e.clientX - this.canvas.width / 2) / (this.canvas.height / 2);
        const mouseY =
          -(e.clientY - this.canvas.height / 2) / (this.canvas.height / 2);

        mat3.translate(this.matrix, this.matrix, [mouseX, mouseY]);
        mat3.scale(this.matrix, this.matrix, [scaleFactor, scaleFactor]);
        mat3.translate(this.matrix, this.matrix, [-mouseX, -mouseY]);
      },
      { passive: true }
    );
    let mouseDown = false;
    document.addEventListener('mousedown', e => {
      if (e.button === 0) mouseDown = true;
    });
    document.addEventListener('mouseup', e => {
      if (e.button === 0) mouseDown = false;
    });
    document.addEventListener(
      'mousemove',
      e => {
        if (mouseDown)
          mat3.translate(this.matrix, this.matrix, [
            e.movementX / (this.canvas.height / -2),
            e.movementY / (this.canvas.height / 2)
          ]);
      },
      { passive: true }
    );
    document.addEventListener('keydown', e => {
      switch (e.key) {
        case ' ':
          mat3.fromScaling(this.matrix, [2, 2]);
          break;
      }
    });

    const loop = () => {
      requestAnimationFrame(loop);
      this.draw();
    };
    requestAnimationFrame(loop);
  }

  load() {
    return Promise.all([
      (async () =>
        (this.vertShaderSrc = await fetchText('./main.vert.glsl')))(),
      (async () => (this.fragShaderSrc = await fetchText('./main.frag.glsl')))()
    ]);
  }

  initGL() {
    const gl =
      this.canvas.getContext('webgl') ||
      this.canvas.getContext('experimental-webgl');
    if (!gl) throw new Error('Could not create webgl context');
    this.gl = gl;

    const vertShader = gl.createShader(gl.VERTEX_SHADER);
    gl.shaderSource(vertShader, this.vertShaderSrc);
    gl.compileShader(vertShader);
    if (!gl.getShaderParameter(vertShader, gl.COMPILE_STATUS))
      throw new Error(
        'Could not compile vertex shader: ' + gl.getShaderInfoLog(vertShader)
      );

    const fragShader = gl.createShader(gl.FRAGMENT_SHADER);
    gl.shaderSource(fragShader, this.fragShaderSrc);
    gl.compileShader(fragShader);
    if (!gl.getShaderParameter(fragShader, gl.COMPILE_STATUS))
      throw new Error(
        'Could not compile fragment shader: ' + gl.getShaderInfoLog(fragShader)
      );

    const program = gl.createProgram();
    gl.attachShader(program, vertShader);
    gl.attachShader(program, fragShader);
    gl.linkProgram(program);
    if (!gl.getProgramParameter(program, gl.LINK_STATUS))
      throw new Error(
        'Could not link shader program: ' + gl.getProgramInfoLog(program)
      );
    gl.useProgram(program);

    const vertices = [1, 1, -1, 1, -1, -1, 1, -1];
    const vertBuffer = gl.createBuffer();
    gl.bindBuffer(gl.ARRAY_BUFFER, vertBuffer);
    gl.bufferData(gl.ARRAY_BUFFER, new Float32Array(vertices), gl.STATIC_DRAW);

    const positionAttribLocation = gl.getAttribLocation(program, 'position');
    gl.vertexAttribPointer(
      positionAttribLocation,
      2,
      gl.FLOAT,
      false,
      2 * Float32Array.BYTES_PER_ELEMENT,
      0
    );
    gl.enableVertexAttribArray(positionAttribLocation);

    this.matrixUniformLocation = gl.getUniformLocation(program, 'matrix');
  }

  resize() {
    const w = innerWidth,
      h = innerHeight;

    this.aspect = w / h;
    this.canvas.width = w;
    this.canvas.height = h;

    this.gl.viewport(0, 0, w, h);
  }

  draw() {
    const gl = this.gl;

    gl.uniformMatrix3fv(
      this.matrixUniformLocation,
      false,
      mat3.mul(mat3.create(), this.matrix, math.aspectCorrection(this.aspect))
    );

    gl.clearColor(0, 0, 0, 1);
    gl.clear(gl.COLOR_BUFFER_BIT);

    gl.drawArrays(gl.TRIANGLE_FAN, 0, 4);
  }
}

const fetchText = (...params) => fetch(...params).then(r => r.text());