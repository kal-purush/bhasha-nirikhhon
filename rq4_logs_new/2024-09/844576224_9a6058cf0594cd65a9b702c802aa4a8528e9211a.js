import * as THREE from "three";


function Light() {
    this.hemisphereLight = new THREE.HemisphereLight(0xFFFFFF,0x888888, 0.5);

    this.directionalLight = new THREE.DirectionalLight(0xFFFFFF, 1.5);
    this.directionalLight.position.set(400, 800, 800);

    this.directionalLight.castShadow = true;
    const length = 1000;
    this.directionalLight.shadow.camera.left = -length;
    this.directionalLight.shadow.camera.right = length;
    this.directionalLight.shadow.camera.top = length;
    this.directionalLight.shadow.camera.bottom = -length;
    this.directionalLight.shadow.camera.far = 1200;
    this.directionalLight.shadow.camera.near = 400;

    this.directionalLight.shadow.mapSize.width = 1024*4;
    this.directionalLight.shadow.mapSize.height = 1024*4;
}

export default Light;