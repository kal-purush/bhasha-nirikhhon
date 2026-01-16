module SCENARIOS {
    export class Scenarios {
		public getDefaultSzenario(scene :BABYLON.Scene) :Array<ORBIT_SPHERE.Sphere>{
			return this.getTwoStar(scene);
		}
        
        public getFourStarSymetric(scene :BABYLON.Scene) :Array<ORBIT_SPHERE.Sphere>{
			var planets = new Array<ORBIT_SPHERE.Sphere>();
            planets.push(new ORBIT_SPHERE.Sphere(6, new BABYLON.Vector3(0,0,0), new BABYLON.Vector3(0,0,0), scene));
                      
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(9, 9, 0), new BABYLON.Vector3(0.3, -0.3, 0), scene));
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(-9, -9, 0), new BABYLON.Vector3(-0.3, 0.3, 0), scene));
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(0, 9, 9), new BABYLON.Vector3(0, 0.3, -0.3), scene));
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(0, -9, -9), new BABYLON.Vector3(0, -0.3, 0.3), scene));

			return planets;
		}
        
        public getTwoStarSymetric(scene :BABYLON.Scene) :Array<ORBIT_SPHERE.Sphere>{
			var planets = new Array<ORBIT_SPHERE.Sphere>();
            planets.push(new ORBIT_SPHERE.Sphere(6, new BABYLON.Vector3(0,0,0), new BABYLON.Vector3(0,0,0), scene));
                      
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(9, 9, 0), new BABYLON.Vector3(0.3, -0.3, 0), scene));
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(-9, -9, 0), new BABYLON.Vector3(-0.3, 0.3, 0), scene));
            
			return planets;
		}
        
        public getTwoStar(scene :BABYLON.Scene) :Array<ORBIT_SPHERE.Sphere>{
			var planets = new Array<ORBIT_SPHERE.Sphere>();
            planets.push(new ORBIT_SPHERE.Sphere(6, new BABYLON.Vector3(0,0,0), new BABYLON.Vector3(0,0,0), scene));
            //this.planets[0].setIsFixedPosition(); //No gravitational force is effecting this guy :)
                      
            planets.push(new ORBIT_SPHERE.Sphere(2, new BABYLON.Vector3(9, 9, 0), new BABYLON.Vector3(0.5, -0.5, 0), scene));

			return planets;
		}
	}
}