///<reference path="../babylon.2.1.d.ts"/>

module DECAYING_GEOMETRY {
    class DecayingGeometry {
        private mesh :BABYLON.Mesh;
        
        private destroyed :boolean = false;
        private lifeTime = 0;
		private createdTime = 0;
		
        constructor(position :BABYLON.Vector3, lifeTime :number, scene :BABYLON.Scene) {
			this.lifeTime = lifeTime;
			this.createdTime = this.getTimestamp();
            this.mesh = BABYLON.Mesh.CreateSphere("sphere", 1, 0.1, scene);
            this.mesh.material = new BABYLON.StandardMaterial("", scene);
            this.setPosition(position);
        }
        
        private getTimestamp() : number{
            var date = new Date();
            return date.getTime();
        }
        
        public isDestroyed() :boolean{
            return this.destroyed;
        }
        
        public setDestroyed() :void{
            this.destroyed = true;
            this.mesh.getScene().removeMesh(this.mesh); //Todo: more to dispose?
        }
        
        public getPosition():BABYLON.Vector3 {
            return this.mesh.position;
        }

        public setPosition(value :BABYLON.Vector3) :void{
            this.mesh.position.x = value.x;
            this.mesh.position.y = value.y;
            this.mesh.position.z = value.z;
        }

        public update() :void{
            var elapsedTime :number = this.getTimestamp() - this.createdTime;
            if(elapsedTime >= this.lifeTime)
                this.setDestroyed();
            else{    
                var quotient :number = elapsedTime / this.lifeTime;
                quotient = 1 - quotient;
                
                this.mesh.material.alpha = quotient;
            }
        }
    }
    
    export class DecayingGeometryManager {
        private lifeTime = 0;
        private geometry: Array<DecayingGeometry>;
        private scene :BABYLON.Scene;
        
        constructor(lifeTime :number, scene :BABYLON.Scene) {
			this.lifeTime = lifeTime;
            this.geometry = new Array<DecayingGeometry>();
            this.updateLoop(this);
            this.scene = scene;
        }
        
        public spawn(position :BABYLON.Vector3) :void{
            this.geometry.push(new DecayingGeometry(position, this.lifeTime, this.scene))
        }
        
        private isUpdateing :boolean = false;
        private updateLoop(mgr : DecayingGeometryManager) : void{
            setTimeout(function(){ mgr.updateLoop(mgr); }, 1000 / 10);
            
            if(mgr.isUpdateing == false){
                mgr.isUpdateing = true;
                
                for (var i = 0; i < mgr.geometry.length; i++) {
                     var element = mgr.geometry[i];
                     if(element.isDestroyed()){
                        mgr.geometry.splice(i, 1);
                        i--;   
                     }
                     else{
                        element.update(); 
                     }                    
                }
                
                mgr.isUpdateing = false;
            }
            else{
                console.log("DecayingGeometryManager: Skipped a frame! Too much load?")
            }
        }
    }
}