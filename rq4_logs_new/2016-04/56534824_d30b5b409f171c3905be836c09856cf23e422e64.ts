export class Render {

	private canvas: HTMLCanvasElement;
	private engine: BABYLON.Engine;
	private scene: BABYLON.Scene;

	constructor() {
		this.canvas = <HTMLCanvasElement>document.getElementById("canvas");
		this.engine = new BABYLON.Engine(this.canvas, true);
		this.scene = this.createScene();
		// run the render loop
		this.engine.runRenderLoop(() => {
			this.scene.render();

		});

		// the canvas/window resize event handler
		window.addEventListener("resize", () => {
			this.engine.resize();
		});
	}

	private createScene (): BABYLON.Scene {
		// create a basic BJS Scene object
		let scene = new BABYLON.Scene(this.engine);

		// create a FreeCamera, and set its position to (x:0, y:5, z:-10)
		let camera = new BABYLON.FreeCamera("camera1", new BABYLON.Vector3(0, 5, -10), scene);

		// target the camera to scene origin
		camera.setTarget(BABYLON.Vector3.Zero());

		// attach the camera to the canvas
		camera.attachControl(this.canvas, false);

		// create a basic light, aiming 0,1,0 - meaning, to the sky
		let light = new BABYLON.HemisphericLight("light1", new BABYLON.Vector3(0, 1, 0), scene);

		// create a built-in "sphere" shape; its constructor takes 5 params: name, width, depth, subdivisions, scene
		let sphere = BABYLON.Mesh.CreateSphere("sphere1", 16, 2, scene);
		sphere.material = new BABYLON.StandardMaterial("t1", scene);
		sphere.material.alpha = 0.5;
		sphere.position.y = 5;

		// create a built-in "ground" shape; its constructor takes the same 5 params as the sphere's one
		let ground = BABYLON.Mesh.CreateGround("ground1", 6, 6, 2, scene);

		// return the created scene
		return scene;
	}
}