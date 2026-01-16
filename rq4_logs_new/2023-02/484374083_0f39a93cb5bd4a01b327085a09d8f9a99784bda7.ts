import * as THREE from 'three';
import { OrbitControls } from 'three/examples/jsm/controls/OrbitControls';
import { RGBELoader } from 'three/examples/jsm/loaders/RGBELoader';
import { GLTFLoader } from 'three/examples/jsm/loaders/GLTFLoader';
import Stats from 'three/examples/jsm/libs/stats.module.js';
import { GUI } from 'lil-gui';

import { EffectComposer } from 'three/examples/jsm/postprocessing/EffectComposer.js';
import { RenderPass } from 'three/examples/jsm/postprocessing/RenderPass.js';
import { ShaderPass } from 'three/examples/jsm/postprocessing/ShaderPass.js';
// import { BloomPass } from 'three/examples/jsm/postprocessing/BloomPass.js';
// import { FilmPass } from 'three/examples/jsm/postprocessing/FilmPass.js';

import vertexShader from '../../assets/shader/vertex/basic-cube.vert?raw';
import tintFS from '../../assets/shader/frag/tint.frag?raw';
import sketchFS from '../../assets/shader/frag/sketch.frag?raw';

// according to https://github.com/Yenseaon/ShadertoyInThreejs
export default class Three16 {
  scene: THREE.Scene;
  camera: THREE.PerspectiveCamera;
  renderer: THREE.WebGLRenderer;
  controls: OrbitControls | undefined;
  composer: EffectComposer | undefined;
  gui: GUI = new GUI();
  stats: Stats = Stats();

  axesHelper: THREE.AxesHelper = new THREE.AxesHelper(30);

  clock: THREE.Clock = new THREE.Clock();

  constructor() {
    this.scene = new THREE.Scene();
    this.camera = new THREE.PerspectiveCamera(
      45,
      window.innerWidth / window.innerHeight,
      0.1,
      1000
    );
    this.renderer = new THREE.WebGLRenderer({
      antialias: true,
    });

    this.init();
  }

  init(): void {
    this.initCamera();
    this.initRender();
    this.initScene();
    this.initPostProcess();
    this.initListener();
    this.render();
  }

  initCamera(): void {
    this.camera.position.set(-1.8, 0.6, 2.7);
    this.camera.lookAt(0, 0, 0);
  }

  initRender(): void {
    this.renderer.setPixelRatio(window.devicePixelRatio);
    this.renderer.setSize(window.innerWidth, window.innerHeight);

    this.renderer.toneMapping = THREE.ACESFilmicToneMapping;
    this.renderer.toneMappingExposure = 1;
    this.renderer.outputEncoding = THREE.sRGBEncoding;

    document.body.appendChild(this.renderer.domElement);
    document.body.appendChild(this.stats.dom);

    this.controls = new OrbitControls(this.camera, this.renderer.domElement);
    this.controls.minDistance = 2;
    this.controls.maxDistance = 10;
    this.controls.target.set(0, 0, -0.2);
    this.controls.update();

    this.composer = new EffectComposer(this.renderer);
    this.composer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    this.composer.setSize(window.innerWidth, window.innerHeight);
  }

  initScene(): void {
    this.initHelper();
    this.initEnv();
    this.initGLTF();
  }

  initHelper(): void {
    this.scene.add(this.axesHelper);
  }

  initEnv(): void {
    new RGBELoader()
      .setPath('textures/equirectangular/')
      .load('royal_esplanade_1k.hdr', (texture) => {
        texture.mapping = THREE.EquirectangularReflectionMapping;

        this.scene.background = texture;
        this.scene.environment = texture;
      });
  }

  initGLTF(): void {
    new GLTFLoader()
      .setPath('models/gltf/DamagedHelmet/glTF/')
      .load('DamagedHelmet.gltf', (gltf) => {
        this.scene.add(gltf.scene);
      });
  }

  initPostProcess(): void {
    const renderModel = new RenderPass(this.scene, this.camera);
    this.composer?.addPass(renderModel);

    /*
    // 内置post process
    // strength = 1, kernelSize = 25, sigma = 4, resolution = 256
    const effectBloom = new BloomPass(2);
    this.composer?.addPass(effectBloom);

    const effectFilm = new FilmPass(0.35, 0.95, 2048, 0);
    this.composer?.addPass(effectFilm);
    */

    // 自定义post process
    this.initTintPostProcess();

    this.initSketchPostProcess();
  }

  // 颜色叠加后处理
  initTintPostProcess(): void {
    const TintShader = {
      uniforms: {
        // 后处理中屏幕当成纹理传入到tDiffuse
        tDiffuse: { value: null },
        uTint: { value: null },
      },
      vertexShader: vertexShader,
      fragmentShader: tintFS,
    };

    const tintPass = new ShaderPass(TintShader);
    tintPass.material.uniforms.uTint.value = new THREE.Vector3();
    this.composer?.addPass(tintPass);

    // gui控制
    const tinitShader = this.gui.addFolder('Tint Shader');
    tinitShader
      .add(tintPass.material.uniforms.uTint.value, 'x')
      .min(-1)
      .max(1)
      .step(0.001)
      .name('red');

    tinitShader
      .add(tintPass.material.uniforms.uTint.value, 'y')
      .min(-1)
      .max(1)
      .step(0.001)
      .name('green');

    tinitShader
      .add(tintPass.material.uniforms.uTint.value, 'z')
      .min(-1)
      .max(1)
      .step(0.001)
      .name('blue');
  }

  // 手绘效果后处理
  initSketchPostProcess(): void {
    // sketch Pass
    const SketchShader = {
      uniforms: {
        tDiffuse: { type: 't', value: null },
        iResolution: {
          type: 'v2',
          value: new THREE.Vector2(window.innerWidth, window.innerHeight),
        },
        MAGIC_GRAD_THRESH: { type: 'f', value: 0.01 }, // gradient threshold 梯度阈值
        MAGIC_SENSITIVITY: { type: 'f', value: 10 }, // Sensitivity 敏感度
        MAGIC_COLOR: { type: 'f', value: 0.5 }, // color threshold 颜色阈值
      },
      // 顶点着色器是普通的矩阵变换
      vertexShader: vertexShader,
      fragmentShader: sketchFS,
    };
    const SketchPass = new ShaderPass(SketchShader);
    this.composer?.addPass(SketchPass);

    const sketchShader = this.gui.addFolder('Sketch shader');

    // 调整这个参数，灰色的绘制线会更加明显，越小 细线越多
    sketchShader
      .add(SketchPass.material.uniforms.MAGIC_GRAD_THRESH, 'value')
      .min(0.0001)
      .max(0.1)
      .step(0.001)
      .name('GRAD_THRESH');

    // 加深绘制线，值越小线越深
    sketchShader
      .add(SketchPass.material.uniforms.MAGIC_SENSITIVITY, 'value')
      .min(0)
      .max(25)
      .step(1)
      .name('SENSITIVITY');

    // 更像是一个过滤的颜色，0的时候就是原来的状态，越大，界面越白
    sketchShader
      .add(SketchPass.material.uniforms.MAGIC_COLOR, 'value')
      .min(0)
      .max(1)
      .step(0.1)
      .name('COLOR');
  }

  initListener(): void {
    window.addEventListener('resize', () => {
      this.camera.aspect = window.innerWidth / window.innerHeight;
      this.camera.updateProjectionMatrix();
      this.renderer.setSize(window.innerWidth, window.innerHeight);

      this.composer?.setSize(window.innerWidth, window.innerHeight);
      this.composer?.setPixelRatio(Math.min(window.devicePixelRatio, 2));
    });
  }

  // 渲染
  render(): void {
    this.animate();
  }

  animate(): void {
    // this.renderer.render(this.scene, this.camera);
    this.composer?.render(0.01);

    this.stats.update();

    const delta = this.clock.getDelta();

    requestAnimationFrame(() => {
      this.animate();
    });
  }
}