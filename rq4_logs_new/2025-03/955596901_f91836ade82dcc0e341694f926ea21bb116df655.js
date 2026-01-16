import * as THREE from 'three';
import { OrbitControls } from 'three/addons/controls/OrbitControls.js';
import { GLTFLoader } from 'three/addons/loaders/GLTFLoader.js';
import { RGBELoader } from 'three/addons/loaders/RGBELoader.js';
import * as CANNON from 'cannon-es';
import { config } from './config.js';

// Global variables
let scene, camera, renderer, world, controls;
let garage, carModels = {};
let currentCar = 'muscle';
let currentMenu = 'main';
let showroomCar, carBody;
let isGameStarted = false;
let loadingProgress = 0;
let gsapAnimations = {};

// Free roam environment variables
let freeRoamActive = false;
let playerCar = null;
let cityElements = [];
let moveDirection = new THREE.Vector3();
let carVelocity = new THREE.Vector3();
let carRotation = 0;
const ACCELERATION = 0.005;
const MAX_SPEED = 0.5;
const FRICTION = 0.98;
const TURN_SPEED = 0.03;

// Initialize the application
init();

// Main initialization function
async function init() {
    initLoading();
    await initThreeJS();
    initControls();
    await createEnvironment();
    initCars();
    initEventListeners();
    animate();
    
    // Hide loading screen and show main menu after initialization
    setTimeout(() => {
        document.querySelector('.loading-screen').style.opacity = '0';
        document.querySelector('.menu').style.opacity = '1';
        setTimeout(() => {
            document.querySelector('.loading-screen').style.display = 'none';
        }, 1000);
    }, 1000);
}

// Initialize loading animation
function initLoading() {
    // Simulate loading progress
    const loadingInterval = setInterval(() => {
        loadingProgress += Math.random() * 15;
        if (loadingProgress >= 100) {
            loadingProgress = 100;
            clearInterval(loadingInterval);
            document.querySelector('.loading-text').textContent = 'Press Any Key to Continue';
        }
        updateLoadingBar();
    }, 500);
    
    // Update loading bar width based on progress
    function updateLoadingBar() {
        const progressPercent = Math.min(loadingProgress, 100);
        document.querySelector('.loading-bar').style.width = `${progressPercent}%`;
        
        if (progressPercent < 100) {
            document.querySelector('.loading-text').textContent = `Loading Assets... ${Math.floor(progressPercent)}%`;
        }
    }
}

// Initialize Three.js scene
async function initThreeJS() {
    // Create scene
    scene = new THREE.Scene();
    scene.background = new THREE.Color(0x121212);
    scene.fog = new THREE.FogExp2(0x000000, config.environment.fogDensity);
    
    // Create camera
    camera = new THREE.PerspectiveCamera(
        config.camera.fov,
        window.innerWidth / window.innerHeight,
        0.1,
        1000
    );
    camera.position.set(...config.camera.initialPosition);
    camera.lookAt(...config.camera.lookAt);
    
    // Create renderer
    renderer = new THREE.WebGLRenderer({
        antialias: true,
        canvas: document.querySelector('#game-canvas'),
        alpha: true
    });
    renderer.setPixelRatio(window.devicePixelRatio);
    renderer.setSize(window.innerWidth, window.innerHeight);
    renderer.shadowMap.enabled = true;
    renderer.shadowMap.type = THREE.PCFSoftShadowMap;
    renderer.toneMapping = THREE.ACESFilmicToneMapping;
    renderer.toneMappingExposure = 1;
    
    // Add ambient light
    const ambientLight = new THREE.AmbientLight(0xffffff, 0.5);
    scene.add(ambientLight);
    
    // Add spotlights
    config.environment.garageSpotlights.forEach(spotConfig => {
        const spotlight = new THREE.SpotLight(spotConfig.color, spotConfig.intensity);
        spotlight.position.set(...spotConfig.position);
        spotlight.castShadow = true;
        spotlight.angle = Math.PI / 4;
        spotlight.penumbra = 0.2;
        scene.add(spotlight);
    });
    
    // Load environment map
    const rgbeLoader = new RGBELoader();
    const envMap = await rgbeLoader.loadAsync('https://raw.githubusercontent.com/mrdoob/three.js/dev/examples/textures/equirectangular/venice_sunset_1k.hdr');
    envMap.mapping = THREE.EquirectangularReflectionMapping;
    scene.environment = envMap;
}

// Initialize controls
function initControls() {
    controls = new OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    controls.minDistance = 5;
    controls.maxDistance = 15;
    controls.maxPolarAngle = Math.PI / 2;
    controls.target.set(0, 1, 0);
}

// Create environment
async function createEnvironment() {
    // Create floor
    const floorGeometry = new THREE.PlaneGeometry(50, 50);
    const floorMaterial = new THREE.MeshStandardMaterial({
        color: 0x111111,
        metalness: 0.9,
        roughness: 0.1
    });
    const floor = new THREE.Mesh(floorGeometry, floorMaterial);
    floor.rotation.x = -Math.PI / 2;
    floor.receiveShadow = true;
    scene.add(floor);
    
    // Create walls
    const wallMaterial = new THREE.MeshStandardMaterial({
        color: 0x222222,
        metalness: 0.5,
        roughness: 0.5
    });
    
    // Back wall
    const backWall = new THREE.Mesh(
        new THREE.PlaneGeometry(50, 20),
        wallMaterial
    );
    backWall.position.z = -25;
    backWall.receiveShadow = true;
    scene.add(backWall);
    
    // Side walls
    const leftWall = backWall.clone();
    leftWall.position.set(-25, 0, 0);
    leftWall.rotation.y = Math.PI / 2;
    scene.add(leftWall);
    
    const rightWall = backWall.clone();
    rightWall.position.set(25, 0, 0);
    rightWall.rotation.y = -Math.PI / 2;
    scene.add(rightWall);
}

// Initialize car models
function initCars() {
    // Create placeholders for car models (in a real game, these would be loaded from 3D model files)
    createMuscleCarModel();
    createSportCarModel();
    createSuperCarModel();
    
    // Set initial car
    showCar(currentCar);
}

// Create muscle car model
function createMuscleCarModel() {
    const carGroup = new THREE.Group();
    
    // Car body
    const bodyGeometry = new THREE.BoxGeometry(4, 1.5, 2);
    const bodyMaterial = new THREE.MeshStandardMaterial({
        color: 0xff0000,
        metalness: 0.7,
        roughness: 0.2
    });
    const body = new THREE.Mesh(bodyGeometry, bodyMaterial);
    body.position.y = 0.75;
    body.castShadow = true;
    carGroup.add(body);
    
    // Car top
    const topGeometry = new THREE.BoxGeometry(2.5, 0.8, 1.8);
    const topMaterial = new THREE.MeshStandardMaterial({
        color: 0xff0000,
        metalness: 0.7,
        roughness: 0.2
    });
    const top = new THREE.Mesh(topGeometry, topMaterial);
    top.position.y = 1.9;
    top.position.z = 0;
    top.castShadow = true;
    carGroup.add(top);
    
    // Wheels
    const wheelGeometry = new THREE.CylinderGeometry(0.4, 0.4, 0.3, 32);
    const wheelMaterial = new THREE.MeshStandardMaterial({
        color: 0x111111,
        metalness: 0.5,
        roughness: 0.7
    });
    
    // Front left wheel
    const wheelFL = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelFL.position.set(-1.7, 0.4, -1);
    wheelFL.rotation.z = Math.PI / 2;
    wheelFL.castShadow = true;
    carGroup.add(wheelFL);
    
    // Front right wheel
    const wheelFR = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelFR.position.set(-1.7, 0.4, 1);
    wheelFR.rotation.z = Math.PI / 2;
    wheelFR.castShadow = true;
    carGroup.add(wheelFR);
    
    // Back left wheel
    const wheelBL = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelBL.position.set(1.7, 0.4, -1);
    wheelBL.rotation.z = Math.PI / 2;
    wheelBL.castShadow = true;
    carGroup.add(wheelBL);
    
    // Back right wheel
    const wheelBR = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelBR.position.set(1.7, 0.4, 1);
    wheelBR.rotation.z = Math.PI / 2;
    wheelBR.castShadow = true;
    carGroup.add(wheelBR);
    
    // Add details (windows, lights, etc.)
    // Front windshield
    const windshieldGeometry = new THREE.PlaneGeometry(1.2, 0.8);
    const windshieldMaterial = new THREE.MeshStandardMaterial({
        color: 0x222222,
        metalness: 0.9,
        roughness: 0.1,
        transparent: true,
        opacity: 0.7
    });
    const windshield = new THREE.Mesh(windshieldGeometry, windshieldMaterial);
    windshield.position.set(-0.8, 1.6, 0);
    windshield.rotation.x = Math.PI / 2;
    windshield.rotation.y = Math.PI / 6;
    carGroup.add(windshield);
    
    // Headlights
    const headlightGeometry = new THREE.CircleGeometry(0.2, 32);
    const headlightMaterial = new THREE.MeshBasicMaterial({ color: 0xffffff });
    
    const headlightL = new THREE.Mesh(headlightGeometry, headlightMaterial);
    headlightL.position.set(-2, 0.8, -0.7);
    headlightL.rotation.y = Math.PI / 2;
    carGroup.add(headlightL);
    
    const headlightR = new THREE.Mesh(headlightGeometry, headlightMaterial);
    headlightR.position.set(-2, 0.8, 0.7);
    headlightR.rotation.y = Math.PI / 2;
    carGroup.add(headlightR);
    
    // Taillights
    const taillightGeometry = new THREE.CircleGeometry(0.2, 32);
    const taillightMaterial = new THREE.MeshBasicMaterial({ color: 0xff0000 });
    
    const taillightL = new THREE.Mesh(taillightGeometry, taillightMaterial);
    taillightL.position.set(2, 0.8, -0.7);
    taillightL.rotation.y = -Math.PI / 2;
    carGroup.add(taillightL);
    
    const taillightR = new THREE.Mesh(taillightGeometry, taillightMaterial);
    taillightR.position.set(2, 0.8, 0.7);
    taillightR.rotation.y = -Math.PI / 2;
    carGroup.add(taillightR);
    
    // Add exhaust pipes
    const exhaustGeometry = new THREE.CylinderGeometry(0.1, 0.1, 0.5, 16);
    const exhaustMaterial = new THREE.MeshStandardMaterial({
        color: 0x777777,
        metalness: 0.9,
        roughness: 0.2
    });
    
    const exhaustL = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustL.position.set(2.2, 0.3, -0.5);
    exhaustL.rotation.z = Math.PI / 2;
    carGroup.add(exhaustL);
    
    const exhaustR = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustR.position.set(2.2, 0.3, 0.5);
    exhaustR.rotation.z = Math.PI / 2;
    carGroup.add(exhaustR);
    
    // Save the car model
    carModels.muscle = {
        model: carGroup,
        bodyMaterial: bodyMaterial,
        topMaterial: topMaterial
    };
}

// Create sport car model
function createSportCarModel() {
    const carGroup = new THREE.Group();
    
    // Car body - more streamlined
    const bodyGeometry = new THREE.BoxGeometry(4.2, 1, 2);
    const bodyMaterial = new THREE.MeshStandardMaterial({
        color: 0x00ff00,
        metalness: 0.8,
        roughness: 0.1
    });
    const body = new THREE.Mesh(bodyGeometry, bodyMaterial);
    body.position.y = 0.5;
    body.castShadow = true;
    carGroup.add(body);
    
    // Car top - sloped
    const topGeometry = new THREE.BoxGeometry(3, 0.7, 1.8);
    const topMaterial = new THREE.MeshStandardMaterial({
        color: 0x00ff00,
        metalness: 0.8,
        roughness: 0.1
    });
    const top = new THREE.Mesh(topGeometry, topMaterial);
    top.position.y = 1.3;
    top.position.z = 0;
    
    // Slope the top for sporty look
    top.geometry.translate(0.5, 0, 0);
    top.rotation.z = -Math.PI / 20;
    top.castShadow = true;
    carGroup.add(top);
    
    // Wheels
    const wheelGeometry = new THREE.CylinderGeometry(0.4, 0.4, 0.25, 32);
    const wheelMaterial = new THREE.MeshStandardMaterial({
        color: 0x111111,
        metalness: 0.5,
        roughness: 0.7
    });
    
    // Front left wheel
    const wheelFL = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelFL.position.set(-1.5, 0.4, -1);
    wheelFL.rotation.z = Math.PI / 2;
    wheelFL.castShadow = true;
    carGroup.add(wheelFL);
    
    // Front right wheel
    const wheelFR = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelFR.position.set(-1.5, 0.4, 1);
    wheelFR.rotation.z = Math.PI / 2;
    wheelFR.castShadow = true;
    carGroup.add(wheelFR);
    
    // Back left wheel
    const wheelBL = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelBL.position.set(1.5, 0.4, -1);
    wheelBL.rotation.z = Math.PI / 2;
    wheelBL.castShadow = true;
    carGroup.add(wheelBL);
    
    // Back right wheel
    const wheelBR = new THREE.Mesh(wheelGeometry, wheelMaterial);
    wheelBR.position.set(1.5, 0.4, 1);
    wheelBR.rotation.z = Math.PI / 2;
    wheelBR.castShadow = true;
    carGroup.add(wheelBR);
    
    // Front bumper/spoiler
    const frontSpoilerGeometry = new THREE.BoxGeometry(0.5, 0.1, 2.2);
    const spoilerMaterial = new THREE.MeshStandardMaterial({
        color: 0x222222,
        metalness: 0.7,
        roughness: 0.3
    });
    const frontSpoiler = new THREE.Mesh(frontSpoilerGeometry, spoilerMaterial);
    frontSpoiler.position.set(-2.2, 0.2, 0);
    frontSpoiler.castShadow = true;
    carGroup.add(frontSpoiler);
    
    // Rear spoiler
    const rearSpoilerStandGeometry = new THREE.BoxGeometry(0.1, 0.5, 1.6);
    const rearSpoilerStand1 = new THREE.Mesh(rearSpoilerStandGeometry, spoilerMaterial);
    rearSpoilerStand1.position.set(1.9, 1, 0);
    carGroup.add(rearSpoilerStand1);
    
    const rearSpoilerTopGeometry = new THREE.BoxGeometry(0.6, 0.1, 2);
    const rearSpoilerTop = new THREE.Mesh(rearSpoilerTopGeometry, spoilerMaterial);
    rearSpoilerTop.position.set(2.1, 1.3, 0);
    carGroup.add(rearSpoilerTop);
    
    // Headlights (more angular)
    const headlightGeometry = new THREE.BoxGeometry(0.1, 0.3, 0.5);
    const headlightMaterial = new THREE.MeshBasicMaterial({ color: 0xffffff });
    
    const headlightL = new THREE.Mesh(headlightGeometry, headlightMaterial);
    headlightL.position.set(-2.1, 0.6, -0.7);
    carGroup.add(headlightL);
    
    const headlightR = new THREE.Mesh(headlightGeometry, headlightMaterial);
    headlightR.position.set(-2.1, 0.6, 0.7);
    carGroup.add(headlightR);
    
    // Taillights (strip style)
    const taillightGeometry = new THREE.BoxGeometry(0.1, 0.2, 1.8);
    const taillightMaterial = new THREE.MeshBasicMaterial({ color: 0xff0000 });
    
    const taillight = new THREE.Mesh(taillightGeometry, taillightMaterial);
    taillight.position.set(2.1, 0.7, 0);
    carGroup.add(taillight);
    
    // Exhaust (dual center)
    const exhaustGeometry = new THREE.CylinderGeometry(0.1, 0.1, 0.3, 16);
    const exhaustMaterial = new THREE.MeshStandardMaterial({
        color: 0xaaaaaa,
        metalness: 0.9,
        roughness: 0.1
    });
    
    const exhaustL = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustL.position.set(2.2, 0.3, -0.3);
    exhaustL.rotation.z = Math.PI / 2;
    carGroup.add(exhaustL);
    
    const exhaustR = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustR.position.set(2.2, 0.3, 0.3);
    exhaustR.rotation.z = Math.PI / 2;
    carGroup.add(exhaustR);
    
    // Save the car model
    carModels.sport = {
        model: carGroup,
        bodyMaterial: bodyMaterial,
        topMaterial: topMaterial
    };
}

// Create super car model
function createSuperCarModel() {
    const carGroup = new THREE.Group();
    
    // Car body - lower and wider
    const bodyGeometry = new THREE.BoxGeometry(4.5, 0.8, 2.2);
    const bodyMaterial = new THREE.MeshStandardMaterial({
        color: 0xff00ff,
        metalness: 1.0,
        roughness: 0.0
    });
    const body = new THREE.Mesh(bodyGeometry, bodyMaterial);
    body.position.y = 0.4;
    body.castShadow = true;
    carGroup.add(body);
    
    // Car top - extremely low profile
    const topGeometry = new THREE.BoxGeometry(2.5, 0.4, 1.8);
    const topMaterial = new THREE.MeshStandardMaterial({
        color: 0xff00ff,
        metalness: 1.0,
        roughness: 0.0
    });
    const top = new THREE.Mesh(topGeometry, topMaterial);
    top.position.y = 0.9;
    top.position.z = 0;
    
    // Extreme slope for supercar look
    top.geometry.translate(0.3, 0, 0);
    top.rotation.z = -Math.PI / 15;
    top.castShadow = true;
    carGroup.add(top);
    
    // Add a hood scoop
    const scoopGeometry = new THREE.BoxGeometry(1, 0.1, 0.8);
    const scoopMaterial = new THREE.MeshStandardMaterial({
        color: 0x000000,
        metalness: 0.7,
        roughness: 0.3
    });
    const scoop = new THREE.Mesh(scoopGeometry, scoopMaterial);
    scoop.position.set(-0.5, 0.85, 0);
    carGroup.add(scoop);
    
    // Wheels - larger rear wheels
    const frontWheelGeometry = new THREE.CylinderGeometry(0.4, 0.4, 0.3, 32);
    const rearWheelGeometry = new THREE.CylinderGeometry(0.45, 0.45, 0.35, 32);
    const wheelMaterial = new THREE.MeshStandardMaterial({
        color: 0x111111,
        metalness: 0.5,
        roughness: 0.7
    });
    
    // Front left wheel
    const wheelFL = new THREE.Mesh(frontWheelGeometry, wheelMaterial);
    wheelFL.position.set(-1.5, 0.4, -1.1);
    wheelFL.rotation.z = Math.PI / 2;
    wheelFL.castShadow = true;
    carGroup.add(wheelFL);
    
    // Front right wheel
    const wheelFR = new THREE.Mesh(frontWheelGeometry, wheelMaterial);
    wheelFR.position.set(-1.5, 0.4, 1.1);
    wheelFR.rotation.z = Math.PI / 2;
    wheelFR.castShadow = true;
    carGroup.add(wheelFR);
    
    // Back left wheel (larger)
    const wheelBL = new THREE.Mesh(rearWheelGeometry, wheelMaterial);
    wheelBL.position.set(1.5, 0.45, -1.1);
    wheelBL.rotation.z = Math.PI / 2;
    wheelBL.castShadow = true;
    carGroup.add(wheelBL);
    
    // Back right wheel (larger)
    const wheelBR = new THREE.Mesh(rearWheelGeometry, wheelMaterial);
    wheelBR.position.set(1.5, 0.45, 1.1);
    wheelBR.rotation.z = Math.PI / 2;
    wheelBR.castShadow = true;
    carGroup.add(wheelBR);
    
    // Front aerodynamic elements
    const frontSplitterGeometry = new THREE.BoxGeometry(0.8, 0.05, 2.4);
    const carbonFiberMaterial = new THREE.MeshStandardMaterial({
        color: 0x222222,
        metalness: 0.3,
        roughness: 0.7
    });
    const frontSplitter = new THREE.Mesh(frontSplitterGeometry, carbonFiberMaterial);
    frontSplitter.position.set(-2.2, 0.15, 0);
    frontSplitter.castShadow = true;
    carGroup.add(frontSplitter);
    
    // Rear wing (large)
    const rearWingStand1Geometry = new THREE.BoxGeometry(0.1, 0.7, 0.1);
    const rearWingStand1 = new THREE.Mesh(rearWingStand1Geometry, carbonFiberMaterial);
    rearWingStand1.position.set(1.8, 1, -0.8);
    carGroup.add(rearWingStand1);
    
    const rearWingStand2Geometry = new THREE.BoxGeometry(0.1, 0.7, 0.1);
    const rearWingStand2 = new THREE.Mesh(rearWingStand2Geometry, carbonFiberMaterial);
    rearWingStand2.position.set(1.8, 1, 0.8);
    carGroup.add(rearWingStand2);
    
    const rearWingGeometry = new THREE.BoxGeometry(1, 0.05, 2.2);
    const rearWing = new THREE.Mesh(rearWingGeometry, carbonFiberMaterial);
    rearWing.position.set(1.8, 1.4, 0);
    carGroup.add(rearWing);
    
    // Modern LED headlights
    const headlightGeometry = new THREE.BoxGeometry(0.05, 0.2, 0.8);
    const headlightMaterial = new THREE.MeshBasicMaterial({ color: 0xccffff });
    
    const headlightL = new THREE.Mesh(headlightGeometry, headlightMaterial);
    headlightL.position.set(-2.25, 0.5, -0.5);
    carGroup.add(headlightL);
    
    const headlightR = new THREE.Mesh(headlightGeometry, headlightMaterial);
    headlightR.position.set(-2.25, 0.5, 0.5);
    carGroup.add(headlightR);
    
    // LED strip taillights
    const taillightGeometry = new THREE.BoxGeometry(0.05, 0.1, 2);
    const taillightMaterial = new THREE.MeshBasicMaterial({ color: 0xff0000 });
    
    const taillight = new THREE.Mesh(taillightGeometry, taillightMaterial);
    taillight.position.set(2.25, 0.6, 0);
    carGroup.add(taillight);
    
    // Quad exhaust
    const exhaustGeometry = new THREE.CylinderGeometry(0.08, 0.08, 0.3, 16);
    const exhaustMaterial = new THREE.MeshStandardMaterial({
        color: 0xcccccc,
        metalness: 0.9,
        roughness: 0.1
    });
    
    const exhaustLL = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustLL.position.set(2.2, 0.25, -0.6);
    exhaustLL.rotation.z = Math.PI / 2;
    carGroup.add(exhaustLL);
    
    const exhaustLR = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustLR.position.set(2.2, 0.25, -0.4);
    exhaustLR.rotation.z = Math.PI / 2;
    carGroup.add(exhaustLR);
    
    const exhaustRL = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustRL.position.set(2.2, 0.25, 0.4);
    exhaustRL.rotation.z = Math.PI / 2;
    carGroup.add(exhaustRL);
    
    const exhaustRR = new THREE.Mesh(exhaustGeometry, exhaustMaterial);
    exhaustRR.position.set(2.2, 0.25, 0.6);
    exhaustRR.rotation.z = Math.PI / 2;
    carGroup.add(exhaustRR);
    
    // Save the car model
    carModels.super = {
        model: carGroup,
        bodyMaterial: bodyMaterial,
        topMaterial: topMaterial
    };
}

// Create city environment for free roam
function createFreeRoamEnvironment() {
    // Create ground plane
    const groundGeometry = new THREE.PlaneGeometry(1000, 1000);
    const groundMaterial = new THREE.MeshStandardMaterial({
        color: 0x333333,
        roughness: 0.8,
        metalness: 0.2
    });
    const ground = new THREE.Mesh(groundGeometry, groundMaterial);
    ground.rotation.x = -Math.PI / 2;
    ground.receiveShadow = true;
    scene.add(ground);
    cityElements.push(ground);

    // Add buildings
    for (let i = 0; i < 50; i++) {
        const height = Math.random() * 30 + 10;
        const width = Math.random() * 10 + 5;
        const depth = Math.random() * 10 + 5;
        
        const buildingGeometry = new THREE.BoxGeometry(width, height, depth);
        const buildingMaterial = new THREE.MeshStandardMaterial({
            color: Math.random() * 0x808080 + 0x808080,
            metalness: 0.8,
            roughness: 0.2
        });
        
        const building = new THREE.Mesh(buildingGeometry, buildingMaterial);
        building.position.x = Math.random() * 200 - 100;
        building.position.z = Math.random() * 200 - 100;
        building.position.y = height / 2;
        building.castShadow = true;
        building.receiveShadow = true;
        
        scene.add(building);
        cityElements.push(building);
    }

    // Add street lights
    for (let i = 0; i < 30; i++) {
        const lightGeometry = new THREE.CylinderGeometry(0.2, 0.2, 8, 8);
        const lightMaterial = new THREE.MeshStandardMaterial({
            color: 0x666666,
            metalness: 0.8,
            roughness: 0.2
        });
        
        const lampPost = new THREE.Mesh(lightGeometry, lightMaterial);
        lampPost.position.x = Math.random() * 180 - 90;
        lampPost.position.z = Math.random() * 180 - 90;
        lampPost.position.y = 4;
        
        const light = new THREE.PointLight(0xffff99, 1, 20);
        light.position.set(0, 8, 0);
        lampPost.add(light);
        
        scene.add(lampPost);
        cityElements.push(lampPost);
    }
}

// Start free roam mode
function startFreeRoam() {
    freeRoamActive = true;
    
    // Hide menus
    document.querySelector('.menu-container').style.display = 'none';
    
    // Create environment
    createFreeRoamEnvironment();
    
    // Setup player car
    playerCar = carModels[currentCar].model.clone();
    playerCar.position.set(0, 0.5, 0);
    scene.add(playerCar);
    
    // Reset camera
    camera.position.set(0, 3, -10);
    controls.enabled = false;
    
    // Add event listeners for controls
    window.addEventListener('keydown', handleDriveControls);
    window.addEventListener('keyup', handleDriveControls);
}

// Handle driving controls
function handleDriveControls(event) {
    if (!freeRoamActive) return;
    
    const keyDown = event.type === 'keydown';
    
    switch(event.code) {
        case 'ArrowUp':
        case 'KeyW':
            moveDirection.z = keyDown ? -1 : 0;
            break;
        case 'ArrowDown':
        case 'KeyS':
            moveDirection.z = keyDown ? 1 : 0;
            break;
        case 'ArrowLeft':
        case 'KeyA':
            moveDirection.x = keyDown ? -1 : 0;
            break;
        case 'ArrowRight':
        case 'KeyD':
            moveDirection.x = keyDown ? 1 : 0;
            break;
        case 'Escape':
            if (keyDown) exitFreeRoam();
            break;
    }
}

// Exit free roam mode
function exitFreeRoam() {
    freeRoamActive = false;
    
    // Remove city elements
    cityElements.forEach(element => scene.remove(element));
    cityElements = [];
    
    // Remove player car
    if (playerCar) {
        scene.remove(playerCar);
        playerCar = null;
    }
    
    // Reset camera and controls
    camera.position.set(...config.camera.initialPosition);
    controls.enabled = true;
    
    // Show menus
    document.querySelector('.menu-container').style.display = 'flex';
    switchMenu('main');
    
    // Remove drive controls
    window.removeEventListener('keydown', handleDriveControls);
    window.removeEventListener('keyup', handleDriveControls);
}

// Show selected car
function showCar(carType) {
    // Remove previous car if exists
    if (showroomCar) {
        scene.remove(showroomCar);
    }
    
    // Clone the model so we don't modify the original
    const carData = carModels[carType];
    showroomCar = carData.model.clone();
    
    // Position the car
    showroomCar.position.y = 0.4;
    
    // Add to scene
    scene.add(showroomCar);
    
    // Save references to materials for customization
    carBody = {
        bodyMaterial: carData.bodyMaterial.clone(),
        topMaterial: carData.topMaterial.clone()
    };
    
    // Apply materials to the cloned model
    showroomCar.traverse((child) => {
        if (child.isMesh) {
            if (child.material === carData.bodyMaterial) {
                child.material = carBody.bodyMaterial;
            } else if (child.material === carData.topMaterial) {
                child.material = carBody.topMaterial;
            }
        }
    });
    
    // Add physics body (not fully implemented in this demo)
    addCarPhysics(carType);
    
    // Play car showcase animation
    playCarAnimation(carType);
    
    currentCar = carType;
    
    // Update UI to reflect selected car
    updateCarSelectionUI(carType);
}

// Add physics to car
function addCarPhysics(carType) {
    // In a full game, we would add detailed vehicle physics here
    // This is a placeholder for demonstration purposes
}

// Play car showcase animation based on car type
function playCarAnimation(carType) {
    const animation = config.cars[carType].showcaseAnimation;
    
    // Clear any existing animation
    if (gsapAnimations[carType]) {
        gsapAnimations[carType] = null;
    }
    
    // Reset car position and rotation
    showroomCar.rotation.set(0, 0, 0);
    showroomCar.position.set(0, 0.4, 0);
    
    gsapAnimations[carType] = {
        rotationY: 0,
        positionY: 0.4,
        positionX: 0,
        rotationZ: 0
    };
    
    // Determine which animation to play
    switch (animation) {
        case 'rev':
            anime({
                targets: gsapAnimations[carType],
                positionY: [0.4, 0.45, 0.4],
                rotationY: Math.PI * 2,
                duration: 5000,
                easing: 'easeInOutQuad',
                loop: true,
                update: function() {
                    if (showroomCar) {
                        showroomCar.position.y = gsapAnimations[carType].positionY;
                        showroomCar.rotation.y = gsapAnimations[carType].rotationY;
                    }
                }
            });
            break;
            
        case 'drift':
            anime({
                targets: gsapAnimations[carType],
                rotationY: Math.PI * 2,
                positionX: [0, 0.5, -0.5, 0],
                rotationZ: [0, 0.05, -0.05, 0],
                duration: 6000,
                easing: 'easeInOutSine',
                loop: true,
                update: function() {
                    if (showroomCar) {
                        showroomCar.rotation.y = gsapAnimations[carType].rotationY;
                        showroomCar.position.x = gsapAnimations[carType].positionX;
                        showroomCar.rotation.z = gsapAnimations[carType].rotationZ;
                    }
                }
            });
            break;
            
        case 'spin':
            anime({
                targets: gsapAnimations[carType],
                rotationY: Math.PI * 2,
                duration: 4000,
                easing: 'linear',
                loop: true,
                update: function() {
                    if (showroomCar) {
                        showroomCar.rotation.y = gsapAnimations[carType].rotationY;
                    }
                }
            });
            break;
    }
}

// Update car selection UI
function updateCarSelectionUI(carType) {
    const carOptions = document.querySelectorAll('.car-option');
    carOptions.forEach(option => {
        if (option.dataset.car === carType) {
            option.classList.add('selected');
        } else {
            option.classList.remove('selected');
        }
    });
}

// Change car color
function changeCarColor(color) {
    if (carBody) {
        carBody.bodyMaterial.color.set(color);
        carBody.topMaterial.color.set(color);
    }
}

// Initialize event listeners
function initEventListeners() {
    // Menu option click handlers
    const menuOptions = document.querySelectorAll('.menu-option');
    menuOptions.forEach(option => {
        option.addEventListener('click', () => {
            const menuType = option.dataset.menu;
            
            // Special handler for exit button
            if (menuType === 'exit') {
                // For web demo, ask for confirmation
                if (confirm('Do you want to exit Velocity Rush: Street Legends?')) {
                    window.close();
                    // Fallback if window.close() is blocked
                    document.body.innerHTML = '<div style="display:flex;align-items:center;justify-content:center;height:100vh;"><h1>Thanks for playing Velocity Rush: Street Legends!</h1></div>';
                }
                return;
            }
            
            // Show appropriate submenu
            switchMenu(menuType);
        });
    });
    
    // Back button handlers
    const backButtons = document.querySelectorAll('.back-button');
    backButtons.forEach(button => {
        button.addEventListener('click', () => {
            switchMenu('main');
        });
    });
    
    // Car selection handlers
    const carOptions = document.querySelectorAll('.car-option');
    carOptions.forEach(option => {
        option.addEventListener('click', () => {
            const carType = option.dataset.car;
            showCar(carType);
        });
    });
    
    // Customization tab handlers
    const customizationTabs = document.querySelectorAll('.tab');
    customizationTabs.forEach(tab => {
        tab.addEventListener('click', () => {
            // Remove active class from all tabs
            customizationTabs.forEach(t => t.classList.remove('active'));
            // Add active class to clicked tab
            tab.classList.add('active');
            
            // In a full game, this would show different customization options
            // based on the selected tab
        });
    });
    
    // Color option handlers
    const colorOptions = document.querySelectorAll('.color-option');
    colorOptions.forEach(option => {
        option.addEventListener('click', () => {
            const color = option.dataset.color;
            changeCarColor(color);
            
            // Visual feedback for selection
            colorOptions.forEach(o => o.classList.remove('selected'));
            option.classList.add('selected');
        });
    });
    
    // Window resize event
    window.addEventListener('resize', onWindowResize);
    
    // Handle key presses for shortcuts
    window.addEventListener('keydown', handleKeyPress);
    
    // Add race mode click handlers
    const raceModes = document.querySelectorAll('.race-mode');
    raceModes.forEach(mode => {
        mode.addEventListener('click', () => {
            const modeType = mode.dataset.mode;
            if (modeType === 'free-roam') {
                startFreeRoam();
            }
            // Add other race modes here in the future
        });
    });
}

// Handle key press events
function handleKeyPress(event) {
    // ESC to go back to main menu
    if (event.key === 'Escape') {
        switchMenu('main');
    }
}

// Switch between menus
function switchMenu(menuType) {
    // Hide all submenus
    const subMenus = document.querySelectorAll('.sub-menu');
    subMenus.forEach(menu => {
        menu.classList.remove('active');
    });
    
    if (menuType === 'main') {
        // Show main menu
        document.querySelector('.menu').style.opacity = '1';
        document.querySelector('.menu').style.pointerEvents = 'auto';
        currentMenu = 'main';
    } else {
        // Hide main menu
        document.querySelector('.menu').style.opacity = '0';
        document.querySelector('.menu').style.pointerEvents = 'none';
        
        // Show selected submenu
        const targetMenu = document.getElementById(`${menuType}-menu`);
        if (targetMenu) {
            targetMenu.classList.add('active');
            currentMenu = menuType;
        }
    }
}

// Window resize handler
function onWindowResize() {
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
}

// Animation loop
function animate() {
    requestAnimationFrame(animate);
    
    if (freeRoamActive && playerCar) {
        // Update car physics
        if (moveDirection.z !== 0) {
            carVelocity.z += moveDirection.z * ACCELERATION;
        }
        if (moveDirection.x !== 0) {
            carRotation += moveDirection.x * TURN_SPEED;
            playerCar.rotation.y = carRotation;
        }
        
        // Apply velocity and friction
        carVelocity.multiplyScalar(FRICTION);
        carVelocity.z = Math.min(Math.max(carVelocity.z, -MAX_SPEED), MAX_SPEED);
        
        // Update position
        playerCar.position.x += Math.sin(carRotation) * -carVelocity.z;
        playerCar.position.z += Math.cos(carRotation) * -carVelocity.z;
        
        // Update camera position
        const cameraOffset = new THREE.Vector3(
            Math.sin(carRotation) * 10,
            3,
            Math.cos(carRotation) * 10
        );
        camera.position.copy(playerCar.position).add(cameraOffset);
        camera.lookAt(playerCar.position);
    } else {
        controls.update();
    }
    
    renderer.render(scene, camera);
}

// Simple animation library to avoid external dependencies
const anime = (function() {
    function getDefaultTiming() {
        return {
            duration: 1000,
            easing: 'easeOutElastic',
            delay: 0,
            loop: false
        };
    }
    
    // Basic easing functions
    const easings = {
        linear: t => t,
        easeInQuad: t => t * t,
        easeOutQuad: t => t * (2 - t),
        easeInOutQuad: t => t < 0.5 ? 2 * t * t : -1 + (4 - 2 * t) * t,
        easeInCubic: t => t * t * t,
        easeOutCubic: t => (--t) * t * t + 1,
        easeInOutCubic: t => t < 0.5 ? 4 * t * t * t : (t - 1) * (2 * t - 2) * (2 * t - 2) + 1,
        easeInQuart: t => t * t * t * t,
        easeOutQuart: t => 1 - (--t) * t * t * t,
        easeInOutQuart: t => t < 0.5 ? 8 * t * t * t * t : 1 - 8 * (--t) * t * t * t,
        easeInQuint: t => t * t * t * t * t,
        easeOutQuint: t => 1 + (--t) * t * t * t * t,
        easeInOutQuint: t => t < 0.5 ? 16 * t * t * t * t * t : 1 + 16 * (--t) * t * t * t * t,
        easeInSine: t => 1 - Math.cos(t * Math.PI / 2),
        easeOutSine: t => Math.sin(t * Math.PI / 2),
        easeInOutSine: t => -(Math.cos(Math.PI * t) - 1) / 2,
        easeOutElastic: t => {
            const p = 0.3;
            return Math.pow(2, -10 * t) * Math.sin((t - p / 4) * (2 * Math.PI) / p) + 1;
        }
    };
    
    function interpolate(start, end, progress) {
        if (Array.isArray(start)) {
            // Handle array values (like paths)
            const result = [];
            for (let i = 0; i < Math.min(start.length, end.length); i++) {
                result.push(start[i] + (end[i] - start[i]) * progress);
            }
            return result;
        } else {
            // Simple numeric interpolation
            return start + (end - start) * progress;
        }
    }
    
    function animate(params) {
        const timing = {...getDefaultTiming(), ...params};
        const startTime = performance.now();
        let frameId;
        
        function step(currentTime) {
            const elapsed = currentTime - startTime;
            let progress = Math.min(elapsed / timing.duration, 1);
            
            // Apply easing
            const easingFn = typeof timing.easing === 'string' 
                ? easings[timing.easing] || easings.linear 
                : timing.easing;
            
            const easedProgress = easingFn(progress);
            
            // Update all target properties
            for (const prop in params.targets) {
                if (prop !== 'targets' && !timing.hasOwnProperty(prop)) {
                    let value = params.targets[prop];
                    let endValue = params[prop];
                    
                    // Handle array values for keyframe-like animation
                    if (Array.isArray(endValue)) {
                        // Find the appropriate segment
                        const numSegments = endValue.length - 1;
                        const segmentProgress = easedProgress * numSegments;
                        const segmentIndex = Math.min(Math.floor(segmentProgress), numSegments - 1);
                        const segmentSpecificProgress = segmentProgress - segmentIndex;
                        
                        // Interpolate between the two keyframe values
                        const segmentStart = endValue[segmentIndex];
                        const segmentEnd = endValue[segmentIndex + 1];
                        
                        value = interpolate(segmentStart, segmentEnd, easings.linear(segmentSpecificProgress));
                    } else {
                        // Simple start to end interpolation
                        const startValue = typeof value === 'number' ? value : 0;
                        value = interpolate(startValue, endValue, easedProgress);
                    }
                    
                    params.targets[prop] = value;
                }
            }
            
            // Call update function if provided
            if (timing.update) {
                timing.update();
            }
            
            // Continue animation if not complete
            if (progress < 1) {
                frameId = requestAnimationFrame(step);
            } else if (timing.loop) {
                // Reset and continue if looping
                params.targets = {...timing.targets};
                frameId = requestAnimationFrame(() => step(performance.now()));
            }
        }
        
        // Start animation
        frameId = requestAnimationFrame(step);
        
        // Return control object
        return {
            cancel: () => cancelAnimationFrame(frameId)
        };
    }
    
    return animate;
})();