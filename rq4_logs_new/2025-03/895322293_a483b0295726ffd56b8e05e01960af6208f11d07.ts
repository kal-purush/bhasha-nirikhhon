import * as THREE from 'three';
import { ThreeUserData } from './types';

declare module 'three' {

  interface Material {
    get vUserData(): ThreeUserData;

    set vUserData(userData: Partial<ThreeUserData>);

    updateMultiProbeTexture?(): void;

    onBeforeCompile: (
      parameters: THREE.WebGLProgramParametersWithUniforms,
      renderer: THREE.WebGLRenderer,
    ) => void;

    shader: THREE.WebGLProgramParametersWithUniforms;
    uniforms: { [uniform: string]: THREE.IUniform };
    // defines: {}
  }

}

if (
  !Object.prototype.hasOwnProperty.call(THREE.Material.prototype, 'vUserData')
) {
  Object.defineProperty(THREE.Material.prototype, 'vUserData', {
    get: function () {
      return this.userData as ThreeUserData;
    },
    set: function (userData: Partial<ThreeUserData>) {
      this.userData = { ...this.userData, ...userData };
    },
  });
}