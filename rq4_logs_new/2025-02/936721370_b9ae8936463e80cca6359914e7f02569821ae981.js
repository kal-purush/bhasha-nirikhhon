import * as THREE from "three";
import { GLTFLoader } from "three/examples/jsm/loaders/GLTFLoader";
import { OrbitControls } from "three/examples/jsm/controls/OrbitControls";
import { VRButton } from 'three/examples/jsm/webxr/VRButton.js';
import { XRControllerModelFactory } from 'three/examples/jsm/webxr/XRControllerModelFactory.js';

// Creazione della scena
const scene = new THREE.Scene();
scene.background = new THREE.Color(0x202020); // Sfondo grigio scuro

// Camera
const camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 100);
camera.position.set(0, 2, 5);

// Renderer con ombre e supporto WebXR
const renderer = new THREE.WebGLRenderer({ antialias: true });
renderer.setSize(window.innerWidth, window.innerHeight);
renderer.shadowMap.enabled = true;
renderer.xr.enabled = true; // Abilita WebXR
document.body.appendChild(renderer.domElement);

// Aggiungi il pulsante VR
document.body.appendChild(VRButton.createButton(renderer));

// **Luci**
const ambientLight = new THREE.AmbientLight(0xffffff, 0.5);
scene.add(ambientLight);

const directionalLight = new THREE.DirectionalLight(0xffffff, 2);
directionalLight.position.set(5, 10, 5);
directionalLight.castShadow = true;
scene.add(directionalLight);

// **Piattaforma circolare**
const platformGeometry = new THREE.CylinderGeometry(5, 2, 0.2, 32);
const platformMaterial = new THREE.MeshStandardMaterial({ color: 0x333333 });
const platform = new THREE.Mesh(platformGeometry, platformMaterial);
platform.position.y = 0;
platform.receiveShadow = true;
scene.add(platform);

// **Caricamento modello GLB con ridimensionamento**
const loader = new GLTFLoader();
loader.load("./assets/sci_fi_turret.glb", (gltf) => {
    const model = gltf.scene;

    // **Ridimensiona il modello per adattarlo alla piattaforma**
    const scaleFactor = 0.01; // 🔥 Riduci la scala, prova a modificarlo se serve
    model.scale.set(scaleFactor, scaleFactor, scaleFactor);

    // **Centra il modello sulla piattaforma**
    model.position.set(0, 0.2, 0);

    model.traverse((node) => {
        if (node.isMesh) {
            node.castShadow = true;
            node.receiveShadow = true;
        }
    });

    scene.add(model);
}, undefined, (error) => {
    console.error("Errore nel caricamento del modello:", error);
});

// **Controlli Orbit (disabilitati in VR)**
const controls = new OrbitControls(camera, renderer.domElement);
controls.enableDamping = true;

// **Controller VR**
const controllerModelFactory = new XRControllerModelFactory();

const controller1 = renderer.xr.getController(0);
controller1.addEventListener('selectstart', onSelectStart);
controller1.addEventListener('selectend', onSelectEnd);
scene.add(controller1);

const controller2 = renderer.xr.getController(1);
controller2.addEventListener('selectstart', onSelectStart);
controller2.addEventListener('selectend', onSelectEnd);
scene.add(controller2);

// Modelli dei controller
const controllerGrip1 = renderer.xr.getControllerGrip(0);
controllerGrip1.add(controllerModelFactory.createControllerModel(controllerGrip1));
scene.add(controllerGrip1);

const controllerGrip2 = renderer.xr.getControllerGrip(1);
controllerGrip2.add(controllerModelFactory.createControllerModel(controllerGrip2));
scene.add(controllerGrip2);

// Funzioni per gestire la teleportazione
function onSelectStart(event) {
    const controller = event.target;

    // Crea un raggio per la teleportazione
    const raycaster = new THREE.Raycaster();
    raycaster.set(controller.position, controller.getWorldDirection(new THREE.Vector3()));

    const intersects = raycaster.intersectObjects([platform]);

    if (intersects.length > 0) {
        const point = intersects[0].point;
        camera.position.set(point.x, point.y + 1.6, point.z); // 1.6 è l'altezza media di una persona
    }
}

function onSelectEnd(event) {
    // Puoi aggiungere ulteriori logiche qui se necessario
}

// **Gestione Resize**
window.addEventListener("resize", () => {
    camera.aspect = window.innerWidth / window.innerHeight;
    camera.updateProjectionMatrix();
    renderer.setSize(window.innerWidth, window.innerHeight);
});

// **Loop di animazione**
function animate() {
    renderer.setAnimationLoop(() => {
        controls.update();
        renderer.render(scene, camera);
    });
}
animate();