// MAIN GAME FILE

import Scene = THREE.Scene;
import Renderer = THREE.WebGLRenderer;
import PerspectiveCamera = THREE.PerspectiveCamera;
import CubeGeometry = THREE.CubeGeometry;
import PlaneGeometry = THREE.PlaneGeometry;
import SphereGeometry = THREE.SphereGeometry;
import AxisHelper = THREE.AxisHelper;
import LambertMaterial = THREE.MeshLambertMaterial;
import MeshBasicMaterial = THREE.MeshBasicMaterial;
import Mesh = THREE.Mesh;
import SpotLight = THREE.SpotLight;
import PointLight = THREE.PointLight;
import Control = objects.Control;
import GUI = dat.GUI;
import Color = THREE.Color;
import Vector3 = THREE.Vector3;

var scene: Scene;
var renderer: Renderer;
var camera: PerspectiveCamera;
var cubeGeometry: CubeGeometry;
var planeGeometry: PlaneGeometry;
var sphereGeometry: SphereGeometry;
var cubeMaterial: MeshBasicMaterial;
var planeMaterial: MeshBasicMaterial;
var sphereMaterial: MeshBasicMaterial;
var axes:AxisHelper;
var cube: Mesh;
var plane: Mesh;
var sphere: Mesh;
var spotLight: SpotLight;
var pointLight: PointLight;
var control: Control;
var gui: GUI;
var stats:Stats;

function init() {
    // Instantiate a new Scene object
	scene = new Scene();
	
	setupRenderer(); // setup the default renderer
	
	setupCamera(); // setup the camera
	
    // add an axis helper to the scene
    axes = new AxisHelper(20);
    scene.add(axes);
    
    //Add a Plane to the Scene
	planeGeometry = new PlaneGeometry(60, 20, 1, 1);
	planeMaterial = new MeshBasicMaterial({color: 0xCCCCCC});
	plane = new Mesh(planeGeometry, planeMaterial);
	plane.receiveShadow = true;
	
	plane.rotation.x = -0.5 * Math.PI;
    plane.position.x = 15;
	plane.position.y = 0;
    plane.position.z = 0;
	
	scene.add(plane);
	console.log("Added Plane Primitive to scene...");
    
    //Add a Cube to the Scene
	cubeGeometry = new CubeGeometry(6, 6, 6);
	cubeMaterial = new MeshBasicMaterial({color:0xFF0000, wireframe:true});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    
    cube.position.x = -4;
    cube.position.y = 3;
    cube.position.z = 0;
    
	scene.add(cube);
	console.log("Added Cube Primitive to scene...");
	
    //Add a Sphere to the Scene
    sphereGeometry = new SphereGeometry(4, 20, 20);
    sphereMaterial = new MeshBasicMaterial({color: 0x7777ff, wireframe:true});
    sphere = new Mesh(sphereGeometry, sphereMaterial);
    sphere.castShadow = true;
    
    sphere.position.x = 20;
    sphere.position.y = 4;
    sphere.position.z = 2;
    
    scene.add(sphere);
    console.log("Added Sphere Primitive to scene");
    
	// Add a SpotLight to the scene
	spotLight = new SpotLight(0xffffff);
	spotLight.position.set (10, 20, 20);
	spotLight.castShadow = true;
	scene.add(spotLight);
	console.log("Added Spot Light to Scene");
	
	document.body.appendChild(renderer.domElement);
	gameLoop(); // render the scene	
}

function addControl(controlObject: Control):void {
	gui.add(controlObject, 'rotationSpeed',-0.01,0.01);
	gui.add(controlObject, 'opacity', 0.1, 1);
	gui.addColor(controlObject, 'color');
}

function addStatsObject() {
	stats = new Stats();
	stats.setMode(0);
	stats.domElement.style.position = 'absolute';
	stats.domElement.style.left = '0px';
	stats.domElement.style.top = '0px';
	document.body.appendChild(stats.domElement);
}

// Setup main game loop
function gameLoop():void {
	stats.update();
	
	// render using requestAnimationFrame
	requestAnimationFrame(gameLoop);
	
	cube.material.transparent = true;
	cube.material.opacity = control.opacity;
	cube.material.color = new Color(control.color);
	cube.rotation.y += control.rotationSpeed;
	
	renderer.render(scene, camera);
}

// Setup default renderer
function setupRenderer():void {
	renderer = new Renderer();
	renderer.setClearColor(0xEEEEEE, 1.0);
	renderer.setSize(window.innerWidth, window.innerHeight);
	renderer.shadowMapEnabled = true;
	console.log("Finished setting up Renderer...");
}

// Setup main camera for the scene
function setupCamera():void {
	camera = new PerspectiveCamera(45, window.innerWidth/window.innerHeight, 0.1, 1000);
	camera.position.x =-30;
	camera.position.y = 40;
	camera.position.z = 30;
	camera.lookAt(scene.position);
	console.log("Finished setting up Camera...");
}