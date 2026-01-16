import "./style.css";
import * as THREE from "three";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls.js";
import * as dat from "dat.gui";

/**
 * Base
 */
// Debug
const gui = new dat.GUI();

// Canvas
const canvas = document.querySelector("canvas.webgl");

// Scene
const scene = new THREE.Scene();

/**
 * Textures
 */
const textureLoader = new THREE.TextureLoader();
const particleTexture = textureLoader.load(
  "../static/textures/particles/5.png"
);
//Particles
const particlesGeometry = new THREE.BufferGeometry();
const count = 20000;

const position = new Float32Array(count * 3);
const colors = new Float32Array(count * 3);
for (let i = 0; i < count * 3; i++) {
  position[i] = (Math.random() - 0.5) * 10;
  colors[i] = Math.random();
}

particlesGeometry.setAttribute(
  "position",
  new THREE.BufferAttribute(position, 3)
);
particlesGeometry.setAttribute("color", new THREE.BufferAttribute(colors, 3));

//material
const particlesMaterial = new THREE.PointsMaterial();
particlesMaterial.size = 0.1;
particlesMaterial.sizeAttenuation = true;
// particlesMaterial.color = new THREE.Color("#ff88cc");
// particlesMaterial.map = particleTexture;
//解决例子边缘黑色关键
particlesMaterial.transparent = true;
particlesMaterial.alphaMap = particleTexture;
//第一种
// particlesMaterial.alphaTest = 0.001;
//第二种方式
// .depthTest : Boolean
// 是否在渲染此材质时启用深度测试。默认为 true。
// particlesMaterial.depthTest = false;
// .depthWrite : Boolean
// 渲染此材质是否对深度缓冲区有任何影响。默认为true。
particlesMaterial.depthWrite = false;
// .vertexColors : Boolean
// 是否使用顶点着色。默认值为false。
particlesMaterial.vertexColors = true;
// .blending : Blending
// 在使用此材质显示对象时要使用何种混合。
// 必须将其设置为CustomBlending才能使用自定义blendSrc, 
// blendDst 或者 [page:Constant blendEquation]。 混合模式所有可能的取值请参阅constants。默认值为NormalBlending。
particlesMaterial.blending = THREE.AdditiveBlending

// Points
const particles = new THREE.Points(particlesGeometry, particlesMaterial);
scene.add(particles);

// //cube
// const cube = new THREE.Mesh(
//   new THREE.BoxGeometry(),
//   new THREE.MeshBasicMaterial()
// );
// scene.add(cube);

/**
 * Sizes
 */
const sizes = {
  width: window.innerWidth,
  height: window.innerHeight,
};

window.addEventListener("resize", () => {
  // Update sizes
  sizes.width = window.innerWidth;
  sizes.height = window.innerHeight;

  // Update camera
  camera.aspect = sizes.width / sizes.height;
  camera.updateProjectionMatrix();

  // Update renderer
  renderer.setSize(sizes.width, sizes.height);
  renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));
});

/**
 * Camera
 */
// Base camera
const camera = new THREE.PerspectiveCamera(
  75,
  sizes.width / sizes.height,
  0.1,
  100
);
camera.position.z = 3;
scene.add(camera);

// Controls
const controls = new OrbitControls(camera, canvas);
controls.enableDamping = true;

/**
 * Renderer
 */
const renderer = new THREE.WebGLRenderer({
  canvas: canvas,
});
renderer.setSize(sizes.width, sizes.height);
renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2));

/**
 * Animate
 */
const clock = new THREE.Clock();
console.log(particlesGeometry.attributes.position.array);
console.log(particlesGeometry.attributes.position.array[0 * 3]);
console.log(particlesGeometry.attributes.position.array[0 * 3 + 0]);
console.log(particlesGeometry.attributes.position.array[0 * 3 + 1]);
console.log(particlesGeometry.attributes.position.array[0 * 3 + 2]);
const tick = () => {
  const elapsedTime = clock.getElapsedTime();

  //Update particles
  for (let i = 0; i < count; i++) {
    const i3 = i * 3;
    const x = particlesGeometry.attributes.position.array[i3];
    particlesGeometry.attributes.position.array[i3 + 1] = Math.sin(
      elapsedTime + x
    );
  }

  particlesGeometry.attributes.position.needsUpdate = true;

  // Update controls
  controls.update();

  // Render
  renderer.render(scene, camera);

  // Call tick again on the next frame
  window.requestAnimationFrame(tick);
};

tick();