// import * as THREE from './three.js-master/build/three.module.js'
// import {GLTFLoader} from './three.js-master/examples/jsm/loaders/GLTFLoader.js'
import * as THREE from 'https://unpkg.com/three@0.126.1/build/three.module.js'
import {GLTFLoader} from 'https://unpkg.com/three@0.126.1/examples/jsm/loaders/GLTFLoader.js'
import {OrbitControls} from 'https://unpkg.com/three@0.126.1/examples/jsm/controls/OrbitControls.js'
import {GUI} from 'https://unpkg.com/three@0.126.1/examples/jsm/libs/dat.gui.module.js'

var camera, scene, renderer, light_1, light_2, model, obj, group_X1, group_X2, earth_group, light_group, orbit_radius, rotationSpeed, X1_scale, X2_scale
orbit_radius = 30
rotationSpeed = 0.01
X1_scale = 1
X2_scale = 1

const canvas = document.querySelector('.webgl')
scene = new THREE.Scene()

// show axes in the screen
// var axes = new THREE.AxisHelper(50);
// scene.add(axes);

const loader = new GLTFLoader()
loader.load('assets/X1_Simplified.glb', function(glb){
    model = glb.scene
    // set the scale of model
    model.scale.set(X1_scale, X1_scale, X1_scale)
    // set the initial position of model
    model.position.x = 0
    // 
    model.position.z = orbit_radius * ((535 + 6400) / 6400)
    // 
    model.rotation.y = -Math.PI / 2
    model.rotation.z = Math.PI / 2
    obj = new THREE.Object3D()
    obj.add(model)
    group_X1 = {model, obj} 
    // obj.castShadow = true;
    scene.add(obj)
}, function(xhr){
    console.log((xhr.loaded / xhr.total * 100) + "% loaded")
}, function(error){
    console.log('An error occured!')
})

loader.load('assets/X2_Simplified.glb', function(glb){
    model = glb.scene
    // set the scale of model
    model.scale.set(X2_scale, X2_scale, X2_scale)
    // set the initial position of model
    model.position.x = 0
    model.position.y = orbit_radius * ((535 + 6400) / 6400) * Math.sin(60 * Math.PI / 180)
    model.position.z = orbit_radius * ((535 + 6400) / 6400) * Math.cos(60 * Math.PI / 180)
    model.rotation.x = -Math.PI / 3
    model.rotation.y = -Math.PI / 2
    model.rotation.z = Math.PI / 2
    obj = new THREE.Object3D()
    obj.add(model)
    group_X2 = {model, obj}
    // group_X2.rotateY(Math.PI * 0.001 / 180)
    // obj.castShadow = true;
    scene.add(obj)
}, function(xhr){
    console.log((xhr.loaded / xhr.total * 100) + "% loaded")
}, function(error){
    console.log('An error occured!')
})


// show the orbit trail on Earth
const curve = new THREE.EllipseCurve(
	0,  0,            // ax, aY
	orbit_radius * ((535 + 6400) / 6400), orbit_radius * ((535 + 6400) / 6400),           // xRadius, yRadius
	0,  2 * Math.PI,  // aStartAngle, aEndAngle
	false,            // aClockwise
	0                 // aRotation
)
const points = curve.getPoints( 50 )
const geometry = new THREE.BufferGeometry().setFromPoints( points )
const material = new THREE.LineBasicMaterial( { color: 0xff0000 } )
const ellipse = new THREE.Line( geometry, material )
ellipse.rotateY(Math.PI / 2)
scene.add(ellipse)

// add lights
light_1 = new THREE.DirectionalLight(0xFFFFAA, 1)
light_1.position.set(1, 2, -4)
light_group = new THREE.Group()
light_group.add(light_1)

scene.add(light_group)

// light_2 = new THREE.DirectionalLight(0xAAFFFF, 1)
// light_2.position.set(-2, -2, -2)
// scene.add(light_2)

const ambientLight = new THREE.AmbientLight(0x020202);
scene.add(ambientLight);

const cubeTextureLoader = new THREE.CubeTextureLoader();
scene.background = cubeTextureLoader.setPath('img/').load([
    'stars.jpg',
    'stars.jpg',
    'stars.jpg',
    'stars.jpg',
    'stars.jpg',
    'stars.jpg'
]);

const textureLoader = new THREE.TextureLoader();

const earthGeo = new THREE.SphereGeometry(30, 30, 30);
const earthMat = new THREE.MeshPhongMaterial({
    map: textureLoader.setPath('img/').load('earthmap4k.jpg')
});
const earth = new THREE.Mesh(earthGeo, earthMat);
earth_group = new THREE.Group() // new THREE.Object3D
earth_group.add(earth)
earth_group.rotateZ(-Math.PI * 24 / 180)
scene.add(earth_group)


/// Boiler Plate Code
const sizes = {
    width: window.innerWidth,
    height: window.innerHeight
}

camera = new THREE.PerspectiveCamera(25, sizes.width/ sizes.height, 0.1, 1000)
camera.position.set(-90, 140, -140)
scene.add(camera)

renderer = new THREE.WebGL1Renderer({
    canvas: canvas
})

renderer.setSize(sizes.width, sizes.height)

// document.body.appendChild(renderer.domElement)

renderer.setPixelRatio(Math.min(window.devicePixelRatio, 2))
renderer.shadowMap.enabled = true
renderer.gammaOutput = true

const controls = new OrbitControls(camera, renderer.domElement);
controls.update()
controls.target.set(0, 0, 0)
controls.enablePan = true
controls.maxPolarAngle = Math.PI * 4
controls.enableDamping = true

// var step = 0

var gui_controls = new function () {
    this.rotationSpeed = 0.01;
    this.X1_scale = 1;
    this.X2_scale = 1;
    
}

var gui = new GUI() 
gui.add(gui_controls, 'rotationSpeed', 0, 0.1)
gui.add(gui_controls, 'X1_scale', 0.1, 2.5)
gui.add(gui_controls, 'X2_scale', 0.1, 2.5)


function animate() {
    earth.rotateY(gui_controls.rotationSpeed / 16)
    // light_group.rotateY(0.001)

    if (group_X1) {
        group_X1.obj.rotateX(-gui_controls.rotationSpeed)
        group_X1.model.scale.set(gui_controls.X1_scale, gui_controls.X1_scale, gui_controls.X1_scale)

    }
    if (group_X1) {
        group_X2.obj.rotateX(-gui_controls.rotationSpeed)
        group_X2.model.scale.set(gui_controls.X2_scale, gui_controls.X2_scale, gui_controls.X2_scale)
    }
    requestAnimationFrame(animate)
    renderer.render(scene, camera)
}
animate()