const main = () => {
    try{
        const RENDERER = new THREE.WebGLRenderer();
        RENDERER.setSize( window.innerWidth, window.innerHeight );
        document.body.appendChild( RENDERER.domElement );

        const FOV = 100;
        const ASPECT = window.innerWidth / window.innerHeight;  // valeur par défaut du canevas
        const NEAR = 0.1;
        const FAR = 1000;
        const CAMERA = new THREE.PerspectiveCamera(FOV, ASPECT, NEAR, FAR)
    
        CAMERA.position.z = 2;
    
        const SCENE = new THREE.Scene()
    
        const BOX_WIDTH = 1
        const BOX_HEIGHT = 1
        const BOX_DEPTH = 1
        const BOX_GEOMETRY = new THREE.BoxGeometry(BOX_WIDTH, BOX_HEIGHT, BOX_DEPTH);
    
        const BOX_MATERIAL = new THREE.MeshPhongMaterial({color: 0x44aa88});  // greenish blue
    
        const CUBE = new THREE.Mesh(BOX_GEOMETRY, BOX_MATERIAL);
    
    
        SCENE.add(CUBE);
    
        const INTENSITY = 1
        const COLOR = 0xFFFFFF
        const LIGHT = new THREE.DirectionalLight(COLOR, INTENSITY)
        LIGHT.position.set(-1, 2, 4)

        SCENE.add(LIGHT)
        RENDERER.render(SCENE, CAMERA);

        const render = (time) => {
            time *= 0.001;  // convertis le temps en secondes
           
            CUBE.rotation.x = time;
            CUBE.rotation.y = time;
           
            RENDERER.render(SCENE, CAMERA);
           
            requestAnimationFrame(render);
          }
          requestAnimationFrame(render);
    } catch (e){
        console.log(e, 'erreur à l\'anim')
    }

    
}

main()