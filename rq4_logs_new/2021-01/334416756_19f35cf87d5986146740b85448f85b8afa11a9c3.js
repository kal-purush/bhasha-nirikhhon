const scene = new THREE.Scene();
const camera = new THREE.PerspectiveCamera(
  75,
  window.innerWidth / window.innerHeight,
  0.1,
  1000
);
const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setClearColor("#e5e5e5");

renderer.setSize(window.innerWidth, window.innerHeight);
document.body.appendChild(renderer.domElement);

window.addEventListener("resize", () => {
  renderer.setSize(window.innerWidth, window.innerHeight);
  camera.aspect = window.innerWidth / window.innerHeight;

  camera.updateProjectMatrix();
});

const geometry = new THREE.BoxGeometry();
const material = new THREE.MeshLambertMaterial({ color: 0x00ff00 });
const cube = new THREE.Mesh(geometry, material);
scene.add(cube);

// camera.position.z = 5;
camera.position.set(0, 1, 2);

const color = 0xffffff;
const intensity = 0.2;
const light = new THREE.PointLight(color, intensity, 500);
light.position.set(10, 0, 25);
scene.add(light);

const light2 = new THREE.HemisphereLight(0xffffbb, 0x080820, 1);
scene.add(light2);
// requestAnimationFrame(animate);

camera.lookAt(cube.position);

var animate = function () {
  requestAnimationFrame(animate);
  // cube.rotation.x += 0.01;
  cube.rotation.y += 0.01;

  renderer.render(scene, camera);
};
animate();