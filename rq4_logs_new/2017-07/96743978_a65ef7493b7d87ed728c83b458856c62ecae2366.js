if (!Detector.webgl) Detector.addGetWebGLMessage();
var container;
var camera, scene, renderer, effect;
var mesh, lightMesh, geometry;
var spheres = [];
var directionalLight, pointLight;
var mouseX = 0,
  mouseY = 0;
var windowHalfX = window.innerWidth / 2;
var windowHalfY = window.innerHeight / 2;
document.addEventListener('mousemove', onDocumentMouseMove, false);

init();
animate();

function init() {
  container = document.createElement('div');
  document.body.appendChild(container);
  container.style.position = 'absolute';
  container.style.zIndex = -1;
  container.style.width = '100%';
  container.style.height = '100%';
  container.style.top = 0;
  container.style.left = 0;

  camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 1, 100000);
  camera.position.z = 3200;
  camera.position.x = -6000;

  scene = new THREE.Scene();
  // 创建由6个图片组成的立方体纹理
  scene.background = new THREE.CubeTextureLoader()
    .setPath('cubeImages/')
    .load(['px.jpg', 'nx.jpg', 'py.jpg', 'ny.jpg', 'pz.jpg', 'nz.jpg']);

  // 创建球面缓冲几何体，第一个参数是半径，第二个参数是水平段数，第三个参数是竖直段数
  var geometry = new THREE.SphereBufferGeometry(100, 32, 16);

  var textureCube = new THREE.CubeTextureLoader()
    .setPath('cubeImages/')
    .load(['px.jpg', 'nx.jpg', 'py.jpg', 'ny.jpg', 'pz.jpg', 'nz.jpg']);

  // 图片如何响应这个对象为立方体折射映照，THREE.CubeReflectionMapping 立方体反射映照，,THREE.SphericalReflectionMapping球面反射映照，THREE.SphericalRefractionMapping球面折射映照
  textureCube.mapping = THREE.CubeRefractionMapping;

  var material = new THREE.MeshBasicMaterial({
    color: 0xffffff,
    envMap: textureCube,
    refractionRatio: 0.95
  });

  for (var i = 0; i < 500; i++) {
    var mesh = new THREE.Mesh(geometry, material);
    mesh.position.x = Math.random() * 10000 - 5000;
    mesh.position.y = Math.random() * 10000 - 5000;
    mesh.position.z = Math.random() * 10000 - 5000;
    mesh.scale.x = mesh.scale.y = mesh.scale.z = Math.random() * 3 + 1;
    scene.add(mesh);
    spheres.push(mesh);
  }

  renderer = new THREE.WebGLRenderer();
  renderer.setPixelRatio(window.devicePixelRatio);
  renderer.setSize(window.innerWidth, window.innerHeight);
  container.appendChild(renderer.domElement);
  // effect = new THREE.StereoEffect(renderer);
  // effect.setSize(window.innerWidth, window.innerHeight);
  window.addEventListener('resize', onWindowResize, false);
}

function onWindowResize() {
  windowHalfX = window.innerWidth / 2;
  windowHalfY = window.innerHeight / 2;
  camera.aspect = window.innerWidth / window.innerHeight;
  camera.updateProjectionMatrix();
  renderer.setSize(window.innerWidth, window.innerHeight);
  // effect.setSize(window.innerWidth, window.innerHeight);
}

function onDocumentMouseMove(event) {
  mouseX = (event.clientX - windowHalfX) * 10;
  mouseY = (event.clientY - windowHalfY) * 10;
}
//
function animate() {
  requestAnimationFrame(animate);
  render();
}

function render() {
  var timer = 0.0001 * Date.now();
  camera.position.x += (mouseX - camera.position.x) * .05;
  camera.position.y += (-mouseY - camera.position.y) * .05;
  camera.lookAt(scene.position);
  for (var i = 0, il = spheres.length; i < il; i++) {
    var sphere = spheres[i];
    sphere.position.x = 5000 * Math.cos(timer + i);
    sphere.position.y = 5000 * Math.sin(timer + i * 1.1);
  }
  renderer.render(scene, camera);
  // effect.render(scene, camera);
}