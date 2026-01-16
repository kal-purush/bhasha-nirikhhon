///<reference path="../../typings/tsd.d.ts" />

class Level extends THREE.Object3D
{
    private cube:THREE.Mesh;
    
    public constructor() 
    {
        console.log("constructor");
        
        super();
    }
    
    public buildScene(scene:THREE.Scene):void 
    {            
        var geometry = new THREE.BoxGeometry(1, 1, 1);
        var material = new THREE.MeshBasicMaterial({color: 0x00ff00});
        
        this.cube = new THREE.Mesh(geometry, material);
        
        scene.add(this.cube);
        
        console.log("buildScene");         
    }

    public render():void 
    {
        this.cube.rotation.x += 0.1;
        this.cube.rotation.y += 0.1;
    }
}