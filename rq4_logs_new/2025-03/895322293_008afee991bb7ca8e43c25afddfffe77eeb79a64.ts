import * as THREE from '../VTHREE.ts';

export interface VMaterial extends THREE.MeshStandardMaterial {
  get shader(): THREE.WebGLProgramParametersWithUniforms;

  set shader(shader: THREE.WebGLProgramParametersWithUniforms);

  get uniforms(): { [uniform: string]: THREE.IUniform };

  clone(): this;

  setUniformByValue(key: string, value: any): void;

  setUniform(key: string, uniform: THREE.Uniform): void;

  setUseLightMapContrast(use: boolean): void;

  getUseLightMapContrast(): boolean;
}