window.onload = function() {
  const num = 30;
  const objects = [];
  const rayCaster = new THREE.Raycaster();
  const mouse = new THREE.Vector2();
  let light;
  let t;

  //create camera
  const camera = new THREE.PerspectiveCamera(
    65,
    window.innerWidth / window.innerHeight,
    0.1,
    1000
  );
  camera.position.set(0.0, 0.0, 5);

  //create scene
  const scene = new THREE.Scene();

  //create renderer
  const renderer = new THREE.WebGLRenderer({ antialias: true });
  renderer.setSize(window.innerWidth, window.innerHeight);

  //append renderer (canvas)
  document.body.appendChild(renderer.domElement);

  //create spotlight
  light = new THREE.SpotLight(0xccddff, 0.8);
  light.position.set(0, 0, 5);

  //adding the light to the scene
  scene.add(light);

  const texture = new THREE.TextureLoader().load("assets/images/texture.png");
  texture.wrapS = texture.wrapT = THREE.RepeatWrapping;
  texture.repeat.set(12, 12);

  //create ground material
  const material = new THREE.MeshPhysicalMaterial({
    map: texture,
    bumpMap: texture
  });

  //create ground mesh
  const geometry = new THREE.PlaneBufferGeometry(100, 100);
  const ground = new THREE.Mesh(geometry, material);
  ground.rotation.z = (Math.PI / 180) * -45;
  ground.rotation.x = (Math.PI / 180) * -90;
  ground.position.y = -2.0;
  scene.add(ground);

  //load object texture
  const objectTexture = new THREE.TextureLoader().load(
    "assets/images/object.png"
  );

  //add cubemap
  const envMap = new THREE.CubeTextureLoader()
    .setPath("assets/images/cubemap/")
    .load(["px.png", "nx.png", "py.png", "ny.png", "pz.png", "nz.png"]);

  //create Tetrahedron
  const geometryTetr = new THREE.TetrahedronBufferGeometry(2, 0);
  const materialTetr = new THREE.MeshPhysicalMaterial({
    map: objectTexture,
    envMap: envMap,
    metalness: -2.0,
    roughness: 5.0
  });

  let tetrahedron = new THREE.Mesh(geometryTetr, materialTetr);
  tetrahedron.rotation.x = (Math.PI / 180) * -10;

  scene.add(tetrahedron);

  //add particles
  for (i = 0; i <= 40; i++) {
    const geometry = new THREE.SphereBufferGeometry(0.1, 6, 6);
    const material = new THREE.MeshPhysicalMaterial({
      envMap: envMap,
      metalness: 1.0
    });
    const particle = new THREE.Mesh(geometry, material);

    //set random position
    particle.position.set(
      Math.random() * 100.0 - 50.0,
      0.0,
      Math.random() * -10
    );

    //calc distance as constants and assign to obj
    const a = new THREE.Vector3(0, 0, 0);
    const b = particle.position;
    const d = a.distanceTo(b);

    particle.distance = d;

    //angles for orbts
    particle.radians = (Math.random() * 360 * Math.PI) / 180;
    particle.radians2 = (Math.random() * 360 * Math.PI) / 180;

    scene.add(particle);
    objects.push(particle);
  }

  //add the animation frame
  const animate = function() {
    for (let i = 0; i < objects.length; i++) {
      const particle = objects[i];
      particle.rotation.y += 0.01;
      if (i % 2 === 0) {
        particle.radians += 0.005;
        particle.radians2 += 0.005;
      } else {
        particle.radians -= 0.005;
        particle.radiaans2 -= 0.005;
      }

      particle.position.x = Math.cos(particle.radians) * particle.distance;
      particle.position.z = Math.sin(particle.radians) * particle.distance;
      particle.position.y =
        Math.sin(particle.radians2) * particle.distance * 0.5;
    }
    tetrahedron.rotation.y -= 0.005;
    camera.lookAt(tetrahedron.position);
    renderer.render(scene, camera);
    requestAnimationFrame(animate);
  };

  //start animation
  animate();
};