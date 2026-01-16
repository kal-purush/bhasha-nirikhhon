/// <reference path="../typings/jquery.d.ts" />
/// <reference path="../typings/three.d.ts" />
/// <reference path="../typings/three-orbitcontrols.d.ts" />
/// <reference path="../typings/stats.d.ts" />
/// <reference path="../typings/three-canvasrenderer.d.ts" />
/// <reference path="../typings/detector.d.ts" />
/// <reference path="../typings/dat-gui.d.ts" />


module App.Rendering {
    export class RenderInstance {
        private container: JQuery;
        private scene: THREE.Scene;
        private camera: THREE.Camera;
        private renderer: THREE.Renderer;
        private controls: THREE.OrbitControls;
        private stats: Stats;
        private cube: THREE.Mesh;
        constructor(attachPoint: JQuery) {
            this.container = attachPoint;
        }
        public init(): void {
            // this.scene
            this.scene = new THREE.Scene();
            // CAMERA
            var SCREEN_WIDTH = window.innerWidth, SCREEN_HEIGHT = window.innerHeight;
            var VIEW_ANGLE = 45, ASPECT = SCREEN_WIDTH / SCREEN_HEIGHT, NEAR = 0.1, FAR = 20000;
            this.camera = new THREE.PerspectiveCamera( VIEW_ANGLE, ASPECT, NEAR, FAR);
            this.scene.add(this.camera);
            this.camera.position.set(0,150,400);
            this.camera.lookAt(this.scene.position);
            // RENDERER
            if (Detector.webgl)
                this.renderer = new THREE.WebGLRenderer( {antialias:true} );
            else
                this.renderer = new THREE.CanvasRenderer();
            this.renderer.setSize(SCREEN_WIDTH, SCREEN_HEIGHT);
            this.container.append(this.renderer.domElement);
            // this.controls
            this.controls = new THREE.OrbitControls( this.camera, this.renderer.domElement );
            // STATS
            this.stats = new Stats();
            this.stats.domElement.style.position = 'absolute';
            this.stats.domElement.style.bottom = '0px';
            this.stats.domElement.style.zIndex = '100';
            this.container.append( this.stats.domElement );
            // LIGHT
            var light = new THREE.PointLight(0xffffff);
            light.position.set(0,250,0);
            this.scene.add(light);
            // FLOOR
            var floorTexture = THREE.ImageUtils.loadTexture('images/checkerboard.jpg');
            floorTexture.wrapS = floorTexture.wrapT = THREE.RepeatWrapping;
            floorTexture.repeat.set( 10, 10 );
            var floorMaterial = new THREE.MeshBasicMaterial( { map: floorTexture, side: THREE.DoubleSide } );
            var floorGeometry = new THREE.PlaneGeometry(1000, 1000, 10, 10);
            var floor = new THREE.Mesh(floorGeometry, floorMaterial);
            floor.position.y = -0.5;
            floor.rotation.x = Math.PI / 2;
            this.scene.add(floor);
            // SKYBOX/FOG
            var skyBoxGeometry = new THREE.CubeGeometry( 10000, 10000, 10000 );
            var skyBoxMaterial = new THREE.MeshBasicMaterial( { color: 0x9999ff, side: THREE.BackSide } );
            var skyBox = new THREE.Mesh( skyBoxGeometry, skyBoxMaterial );
            // this.scene.add(skyBox);
            this.scene.fog = new THREE.FogExp2( 0x9999ff, 0.00025 );

            ////////////
            // CUSTOM //
            ////////////
            var cubeGeometry = new THREE.CubeGeometry( 50, 50, 50 );
            var cubeMaterial = new THREE.MeshBasicMaterial( { color: 0x000088 } );
            this.cube = new THREE.Mesh( cubeGeometry, cubeMaterial );
            this.cube.position.set(0,26,0);
            this.scene.add(this.cube);

            var gui = new dat.GUI();

            var parameters =
            {
                a: 200, // numeric
                b: 200, // numeric slider
                c: "Hello, GUI!", // string
                d: false, // boolean (checkbox)
                e: "#ff8800", // color (hex)
                f: function() { alert("Hello!") },
                g: function() { alert( parameters.c ) },
                v : 0,    // dummy value, only type is important
                w: "...", // dummy value, only type is important
                x: 0, y: 0, z: 0
            };
            // gui.add( parameters )
            gui.add( parameters, 'a' ).name('Number');
            gui.add( parameters, 'b' ).min(128).max(256).step(16).name('Slider');
            gui.add( parameters, 'c' ).name('String');
            gui.add( parameters, 'd' ).name('Boolean');

            gui.addColor( parameters, 'e' ).name('Color');

            var numberList = [1, 2, 3];
            gui.add( parameters, 'v', numberList ).name('List');

            var stringList = ["One", "Two", "Three"];
            gui.add( parameters, 'w', stringList ).name('List');

            gui.add( parameters, 'f' ).name('Say "Hello!"');
            gui.add( parameters, 'g' ).name("Alert Message");

            var folder1 = gui.addFolder('Coordinates');
            folder1.add( parameters, 'x' );
            folder1.add( parameters, 'y' );
            folder1.close();
            gui.open();
        }
        public animate(): void {
            window.requestAnimationFrame(()=> {this.animate();});
            this.update();
            this.render();
        }
        public update(): void {
            this.controls.update();
            this.stats.update();
        }
        public render(): void {
            this.renderer.render(this.scene,this.camera);
        }
    }
}