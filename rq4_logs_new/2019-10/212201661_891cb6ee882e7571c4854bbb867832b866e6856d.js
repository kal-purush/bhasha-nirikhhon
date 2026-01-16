import * as THREE from 'three';
import { Mesh } from 'three/build/three';

export default class Ball {
    constructor() {
        this.createBall();
    }

    createBall() {
        const ball = new THREE.Object3D;

        const ballGeometry = new THREE.SphereGeometry(0.15, 32, 32);
        const ballMaterial = new THREE.MeshLambertMaterial({color: 0xFF0000});
        const mainball = new THREE.Mesh(ballGeometry, ballMaterial);
        ball.add(mainball);
        ball.position.y = -0.8;
        ball.position.z = 2.7;
        ball.position.x = -0.6;

        return ball;
    }
}