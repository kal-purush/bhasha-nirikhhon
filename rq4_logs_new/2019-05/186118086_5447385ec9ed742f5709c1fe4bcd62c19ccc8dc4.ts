

/**
 * Creates a new WaterMaterial Object with a given name. The noiseFile descrips the noise in the water,
 * @param scene
 * @param name
 * @param noiseFile
 */
 export class DefaultWaterMaterial {
   public material: BABYLON.WaterMaterial;

   constructor (scene: BABYLON.Scene, name: string, noiseFile: string) {
     if (!scene) {
         console.error("DefaultWaterMaterial: scene is not defined");
         return;
     }
     if (!name) {
         console.error("DefaultWaterMaterial: name is not defined");
         return;
     }
     if (!noiseFile) {
         console.error("DefaultWaterMaterial: noiseFile is not defined");
         return;
     }

     this.material = new BABYLON.WaterMaterial(name, scene);

     this.material.bumpTexture = new BABYLON.Texture(noiseFile, scene);
     // Water properties
     this.material.windForce = -5;
     this.material.waveHeight = 0;
     this.material.windDirection = new BABYLON.Vector2(1, 1);
     this.material.waterColor = new BABYLON.Color3(0.25, 0.88, 0.82);
     this.material.colorBlendFactor = 0.3;
     this.material.bumpHeight = 0.1;
     this.material.waveLength = 0.1;
   }
 }