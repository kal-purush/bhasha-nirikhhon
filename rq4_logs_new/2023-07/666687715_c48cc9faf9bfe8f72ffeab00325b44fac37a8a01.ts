import vs from './shader/vertex.glsl?raw';
import fs from './shader/fragment.glsl?raw';
import MatX from './minMatrix';
import WebGL from './webGL';

const main = (): void => {
  // get WebGL2 context
  const c: HTMLCanvasElement = document.getElementById('canvas') as HTMLCanvasElement;
  c.width = 512;
  c.height = 512;
  const gl: WebGL2RenderingContext | null = c.getContext('webgl2');
  if (gl == null) {
    throw new Error('Failed to obtain WebGL context');
  }

  // initialize display settings
  const cgl: WebGL = new WebGL();
  cgl.initDisplay(gl, [0, 0, 0, 0]);

  // compile shaders
  const vertexShader: WebGLShader = cgl.createShader(gl, 'VERTEX_SHADER', vs);
  const fragmentShader: WebGLShader = cgl.createShader(gl, 'FRAGMENT_SHADER', fs);

  // create program object & link
  const program: WebGLProgram = cgl.createProgram(gl, vertexShader, fragmentShader);

  // create model
  const position: number[] = [0, 1, 0, 1, -1, 0, -1, -1, 0];
  const pVBO: WebGLBuffer = cgl.createVBO(gl, position);
  cgl.reBindVBO(gl, pVBO);
  cgl.attributeRegist(gl, program, 'position', 3);

  // create color
  const color: number[] = [1.0, 0.0, 0.0, 1.0, 0.0, 1.0, 0.0, 1.0, 0.0, 0.0, 1.0, 1.0];
  const cVBO: WebGLBuffer = cgl.createVBO(gl, color);
  cgl.reBindVBO(gl, cVBO);
  cgl.attributeRegist(gl, program, 'color', 4);

  // matrix transformation
  const m: MatX = new MatX();
  const mMatrix: Float32Array = m.identity(m.create());
  const vMatrix: Float32Array = m.identity(m.create());
  const pMatrix: Float32Array = m.identity(m.create());
  const mvpMatrix: Float32Array = m.identity(m.create());
  m.lookAt([0.0, 1.0, 3.0], [0, 0, 0], [0, 1, 0], vMatrix);
  m.perspective(45, c.width / c.height, 0.1, 100, pMatrix);
  m.multiply(pMatrix, vMatrix, mvpMatrix);
  m.multiply(mvpMatrix, mMatrix, mvpMatrix);

  // uniformLocation
  cgl.uniformLocRegist(gl, program, mvpMatrix, 'mvpMatrix');

  // draw model
  cgl.drawTriangles(gl);
};

window.onload = main;