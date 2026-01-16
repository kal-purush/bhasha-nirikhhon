import * as THREE from 'three';
import "./index.css";

//Scene
const scene = new THREE.Scene();

//create our sphere
const geometry = new THREE.SphereGeometry(3 ,64 ,64);
const material = new THREE.MeshBasicMaterial({color:"#00ff83"});
const mesh = new THREE.Mesh(geometry,material);
scene.add(mesh);

//sizes
const sizes = {
    width: window.innerWidth,
    height:window.innerHeight,
}

//Light
const light = new THREE.PointLight(0xffffff ,1 ,100);
light.position.set(0, 10, 10);
scene.add(light);

/*//direction light
const directionalLight = new THREE.DirectionalLight( 0xffffff, 0.30 );
scene.add( directionalLight );*/

//camera
const camera = new THREE.PerspectiveCamera(45 ,sizes.width/sizes.height);
camera.position.z = 20;
scene.add(camera);

//renderer
const canvas = document.querySelector('.webgl');
const renderer = new THREE.WebGLRenderer({canvas});
renderer.setSize(sizes.width, sizes.height);
renderer.render(scene,camera);





//Resize
window.addEventListener('resize',()=> {
    //update sizes
    sizes.width = window.innerWidth;
    sizes.height = window.innerHeight;
    //ipdate camera
    camera.aspect = sizes.width/sizes.height;
    camera.updateProjectionMatrix();
    renderer.setSize(sizes.width,sizes.height);
});

const loop =()=>{
    renderer.render(scene , camera);
    window.requestAnimationFrame(loop);
}
loop();
