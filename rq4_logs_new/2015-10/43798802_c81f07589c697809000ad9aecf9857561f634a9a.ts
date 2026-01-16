/// <reference path="babylon.2.2.d.ts"/>
/// <reference path="Generator.ts"/>

class GAME {

    //******************************** MEMBERS

    static instance;

    static ASSETS_LIST = [
    ];

    private canvas : HTMLCanvasElement;
    private scene : BABYLON.Scene;
    private light : BABYLON.PointLight;
    private camera : BABYLON.FreeCamera;
    private level : number;
    private assets : BABYLON.Mesh[];

    private gravity : boolean = true;


    //******************************** GET / SET

    getAsset(index) : BABYLON.Mesh {
        return this.assets[index];
    }
    getCanvas() : HTMLCanvasElement {
        return this.canvas;
    }
    getScene() : BABYLON.Scene {
        return this.scene;
    }

    //******************************** CONSTRUCTOR

    constructor(level) {
        GAME.instance = this;
        this.level = level;
        this.init();
    }

    init() {

        // Get the canvas
        this.canvas = <HTMLCanvasElement>document.getElementById("renderCanvas");

        // Create the Babylon engine
        var engine = new BABYLON.Engine(this.canvas, true);

        // Create the scene
        this.scene = new BABYLON.Scene(engine);
        this.scene.clearColor = BABYLON.Color3.Black();
        this.scene.fogMode = BABYLON.Scene.FOGMODE_LINEAR;
        this.scene.fogColor = this.scene.clearColor;
        //this.scene.fogStart = 1.0;
        //this.scene.fogEnd = 5.0;
        this.scene.setGravity(new BABYLON.Vector3(0,-9.81,0));

        // Debug layer
        //this.scene.debugLayer.show();

        this.light = new BABYLON.PointLight(
            "light",
            BABYLON.Vector3.Zero(),
            this.scene
        );

        this.camera = new BABYLON.FreeCamera(
            "camera",
            BABYLON.Vector3.Zero(),
            this.scene
        );
        this.camera.position.y = 25;
        this.camera.keysUp = [90]; // Z
        this.camera.keysLeft = [81]; // Q
        this.camera.keysDown = [83]; // S
        this.camera.keysRight = [68]; // D
        this.camera.attachControl(this.canvas);
        this.light.parent = this.camera;
        this.camera.speed = 0.5;
        this.camera.angularSensibility = 2500;
        this.camera.ellipsoid = new BABYLON.Vector3(1.5,1.5,1.5);
        this.camera.checkCollisions = true;
        this.camera.rotation.x += Math.PI/2;
        //this.camera.applyGravity = true;

        var mazeGenerator = new Generator();
        var exitPoint : BABYLON.Vector3;

        if(this.level == 1) {
            exitPoint = mazeGenerator.generate(42,42,1);
        }
        else if(this.level == 2) {
            exitPoint = mazeGenerator.generate(42,42,2);
        }

        console.log(exitPoint);

        var exitMat = new BABYLON.StandardMaterial("exitMat", this.scene);
        exitMat.diffuseColor = BABYLON.Color3.Red();

        var exit = BABYLON.Mesh.CreateBox(
            "exit", 2,
            this.scene
        );
        exit.material = exitMat;
        exit.position.x = (exitPoint.x * Generator.BLOCK_SIZE);
        //exit.position.y = (exitPoint.y * Generator.BLOCK_SIZE);
        exit.position.y = 8;
        exit.position.z = (exitPoint.z * Generator.BLOCK_SIZE);

        //this.loadAssets(() => {});

        engine.runRenderLoop(() => {
            this.update();
        });

        window.addEventListener("resize", () => {
            engine.resize();
        });

        window.addEventListener("keydown", (evt) => {
            // Press space key to fire
            if (evt.keyCode === 32) {
                if(this.gravity) {
                    this.scene.setGravity(new BABYLON.Vector3(0,9.81,0));
                }
                else {
                    this.scene.setGravity(new BABYLON.Vector3(0,-9.81,0));
                }
            }
        });
    }

    update() {
        this.scene.render();
    }

    //******************************** FUNCTIONS

    /**
     * Load all the assets, then call the callback function
     */
    loadAssets(callback) {
        this.assets = [];
        var count = GAME.ASSETS_LIST.length;

        GAME.ASSETS_LIST.forEach((elem) => {

            BABYLON.SceneLoader.ImportMesh(
                "",
                "assets/models/" + elem + "/",
                elem + ".babylon",
                this.scene,
                (newMeshes) => {

                    this.assets[elem] = BABYLON.Mesh.MergeMeshes(<BABYLON.Mesh[]>newMeshes);
                    this.assets[elem].isVisible = false;
                    this.assets[elem].isPickable = false;

                    count--;
                    if(count == 0) {
                        callback();
                    }
                }
            );
        });
    }

    lookAtDirection(direction) {
        switch (direction) {
            case 0:
                this.camera.rotation.y += Math.PI;
                break;
            case 1:
                this.camera.rotation.y -= Math.PI/2;
                break;
            case 2:
                break;
            case 3:
                this.camera.rotation.y += Math.PI/2;
                break;
            default:
                break;
        }
    }
}