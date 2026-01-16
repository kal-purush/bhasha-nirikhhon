// MAIN GAME FILE

import Scene = THREE.Scene;
import Renderer = THREE.WebGLRenderer;
import PerspectiveCamera = THREE.PerspectiveCamera;
import BoxGeometry = THREE.BoxGeometry;
import CubeGeometry = THREE.CubeGeometry;
import PlaneGeometry = THREE.PlaneGeometry;
import SphereGeometry = THREE.SphereGeometry;
import AxisHelper = THREE.AxisHelper;
import LambertMaterial = THREE.MeshLambertMaterial;
import MeshBasicMaterial = THREE.MeshBasicMaterial;
import Mesh = THREE.Mesh;
import SpotLight = THREE.SpotLight;
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
var cubeMaterial: LambertMaterial;
var planeMaterial: LambertMaterial;
var sphereMaterial: LambertMaterial;
var axes:AxisHelper;
var cube: Mesh;
var parentCube:Mesh;
var plane: Mesh;
var group: Mesh;
var sphere: Mesh;
var spotLight: SpotLight;

var control: Control;
var gui: GUI;
var stats:Stats;
var step:number = 0;

function init() {
    // Instantiate a new Scene object
	scene = new Scene();
	
	setupRenderer(); // setup the default renderer
	
	setupCamera(); // setup the camera
	
    // add an axis helper to the scene
    axes = new AxisHelper(20);
    scene.add(axes);
    
        
    
    //Add a Plane to the Scene
	planeGeometry = new PlaneGeometry(60, 20);
	planeMaterial = new LambertMaterial({color:0xFFFFFF});
	plane = new Mesh(planeGeometry, planeMaterial);
	plane.receiveShadow = true;
	
	plane.rotation.x = -0.5 * Math.PI;
    plane.position.x = 10;
	plane.position.y = 0;
    plane.position.z = 0;
	
	scene.add(plane);
	console.log("Added Plane Primitive to scene...");
    parentCube = new Mesh();
     //torso 
	cubeGeometry = new BoxGeometry(4, 6, 8);
	cubeMaterial = new LambertMaterial({color:0xff0000});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    cube.position.x = -4;
    cube.position.y = 8;
    cube.position.z = 2.5;
    parentCube.add(cube);
   
    
	console.log("Added ParentCube Primitive to scene...");
     //head 
	cubeGeometry = new BoxGeometry(2, 6, 2);
	cubeMaterial = new LambertMaterial({color:0x7f0000});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    cube.position.x = -4;
    cube.position.y = 10;
    cube.position.z = 2.5;
    parentCube.add(cube);
    
    console.log("Added head Primitive to scene...");
    
    //left arm 
	cubeGeometry = new BoxGeometry(1, 1, 8);
	cubeMaterial = new LambertMaterial({color:0xff0000});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    cube.position.x = -4;
    cube.position.y = 8;
    cube.position.z = -0.5;
    parentCube.add(cube);
    
	console.log("Added Cube Primitive to scene...");
    
    //right arm 
	cubeGeometry = new BoxGeometry(1, 1, 8);
	cubeMaterial = new LambertMaterial({color:0xff0000});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    cube.position.x = -4;
    cube.position.y = 8;
    cube.position.z = 5.5;
    parentCube.add(cube);
    
	console.log("Added Cube Primitive to scene...");
    
   
    
    //left leg
	cubeGeometry = new BoxGeometry(3, 10, 2);
	cubeMaterial = new LambertMaterial({color:0x651818});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    
    cube.position.x = -4;
    cube.position.y = 3;
    cube.position.z = 0;
    
	parentCube.add(cube);
    
	console.log("Added Cube Primitive to scene...");
    
     //right leg
	cubeGeometry = new BoxGeometry(3, 10, 2);
	cubeMaterial = new LambertMaterial({color:0x651818});
	cube = new Mesh(cubeGeometry, cubeMaterial);    
	cube.castShadow = true;
    
    cube.position.x = -4;
    cube.position.y = 3;
    cube.position.z = 5;
    
	parentCube.add(cube);
    scene.add(parentCube);
	console.log("Added Cube2 Primitive to scene...");
	
           
	// Add a SpotLight to the scene
	spotLight = new SpotLight(0xbdbd1e);
	spotLight.position.set (-40, 60, -10);
	spotLight.castShadow = true;
	scene.add(spotLight);
	console.log("Added Spot Light to Scene");
	
    //Add Ambient Light to the scene
    var light = new THREE.AmbientLight( 0x404040 ); 
    scene.add( light );
    // add controls
	gui = new GUI();
	control = new Control(0,0,0);
	addControl(control);
    
    // Add framerate stats
    addStatsObject();
    
	document.body.appendChild(renderer.domElement);
	gameLoop(); // render the scene	
}

function addControl(controlObject: Control):void {
	gui.add(controlObject, 'rotateXAxis', 0, 1);
	gui.add(controlObject, 'rotateYAxis', 0, 1);
    gui.add(controlObject, 'rotateZAxis', 0, 1);
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
	
    // //animate cube
     parentCube.rotation.x += control.rotateXAxis*15;
     parentCube.rotation.y += control.rotateYAxis*15;
     parentCube.rotation.z += control.rotateZAxis*15;
    
       
	// render using requestAnimationFrame
	requestAnimationFrame(gameLoop);
	
    // render the scene
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