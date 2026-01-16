let scene, camera, renderer;

function init() {
    scene = new THREE.Scene();
    camera = new THREE.PerspectiveCamera(75, window.innerWidth / window.innerHeight, 0.1, 1000);
    renderer = new THREE.WebGLRenderer();
    renderer.setSize(window.innerWidth, window.innerHeight);
    document.body.appendChild(renderer.domElement);

    const light = new THREE.AmbientLight(0x404040); // мягкий белый свет
    scene.add(light);

    // Загрузчик GLTF для 3D модели
    const loader = new THREE.GLTFLoader();
    loader.load('scene.glb', function(gltf) { // Убедитесь, что путь к модели указан правильно
        scene.add(gltf.scene);
        animate();
    }, undefined, function(error) {
        console.error(error);
    });

    camera.position.z = 5;
}

function animate() {
    requestAnimationFrame(animate);
    renderer.render(scene, camera);
}

init();
