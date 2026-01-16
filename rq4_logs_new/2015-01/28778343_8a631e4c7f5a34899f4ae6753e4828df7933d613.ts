import THREE = require('three');
import m = require('mithril');
import Model = require('./model');

interface Particle {
    velocity: THREE.Vector3;
    index: number;
}

class Explosion {
    sceneObject: THREE.Object3D;
    particles: Particle[] = [];

    private geometry: THREE.Geometry;

    constructor(source: Model){
        this.geometry = new THREE.Geometry();
        source.traverseGeometry(()=> true, (v1,v2,v3) =>
            [v1,v2,v3].forEach(v => this.addParticle(source.sceneObject.position, v)));

        var material = new THREE.PointCloudMaterial({size: 0.2});
        this.sceneObject = new THREE.PointCloud(this.geometry, material);
        this.sceneObject.castShadow = true;
    }

    update(delta: number){
        this.particles.forEach(p =>{
            var v = this.geometry.vertices[p.index];
            p.velocity.multiplyScalar(1 - delta * (p.velocity.length() / 100));
            v.add(p.velocity.clone().multiplyScalar(delta));
        });
        this.geometry.verticesNeedUpdate = true;
    }

    private addParticle (center: THREE.Vector3, v: THREE.Vector3) {
        var random = new THREE.Vector3(0.5 - Math.random(),0.5 - Math.random(),0.5 - Math.random());
        this.particles.push({
            velocity: v.clone().sub(random.add(center)).multiplyScalar(100),
            index: this.geometry.vertices.length
        });

        this.geometry.vertices.push(v.clone())
    }
}
export = Explosion;