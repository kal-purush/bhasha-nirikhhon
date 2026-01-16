'use strict';

interface ITrackConfig {
    startingPosition: { x: number, y: number, orientation: number };
    finishLine: { x: number, y: number, orientation: number, width: number };
}

class Track {
    private mesh: THREE.Object3D;

    constructor(filePath: string, callback: (trackMesh: THREE.Object3D) => void) {
        $.getJSON(filePath, (data: any) => {
            var loader = new THREE.ColladaLoader();
            loader.load('assets/tracks/' + data.model, (result: any) => {
                this.mesh = result.scene;
                this.config = data.config;
                this.mesh.traverse((child: any) => {
                    if (child instanceof THREE.Mesh) {
                        this.hitMesh.push(child);
                    }
                });

                this.finishLineGeometry = new THREE.BoxGeometry(this.config.finishLine.width, 5, 1);
                var material = new THREE.MeshBasicMaterial({ color: 0x00ff00 });
                this.finishLineMesh = new THREE.Mesh(this.finishLineGeometry, material);
                this.finishLineMesh.position.set(this.config.finishLine.x, this.config.finishLine.y, 0);
                this.finishLineMesh.rotateZ(THREE.Math.degToRad(this.config.finishLine.orientation));

                callback(this.mesh);
            });
        });
    }

    public config: ITrackConfig;
    public hitMesh: THREE.Mesh[] = [];
    public finishLineGeometry: THREE.BoxGeometry;
    public finishLineMesh: THREE.Mesh;
}