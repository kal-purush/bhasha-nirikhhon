var DEMO = (function() {
  var LIGHTPARAMS = {
    ambientLight: {
      color: 0xcccccc
    },

    directionalLight: {
      color: 0xffffff,
      intensity: 0.7,
      position: {
        x: -550,
        y: 375,
        z: 10
      }
    }
  };

  var upHouse, plane, background, materials, scene, renderer;
  var state, animationState;
  var animationID = null;

  function init() {
    scene = new THREE.Scene();

    renderer = new THREE.WebGLRenderer({ alpha: true });
    renderer.setSize(window.innerWidth, window.innerHeight);
    TW.mainInit(renderer, scene);
    document.getElementById('webgl-output').appendChild(renderer.domElement);

    // ========================================================================
    // Load textures
    // ========================================================================
    // Sun's rays
    var raysTexture = new THREE.ImageUtils.loadTexture("./textures/rays.png");
    var raysMaterial = new THREE.MeshBasicMaterial({ transparent: true,
                                                     map: raysTexture });
    
    // Plane image
    var planeTexture = new THREE.ImageUtils.loadTexture("./textures/plane.png");
    planeTexture.wrapS = THREE.RepeatWrapping;
    planeTexture.repeat.x = -1; // flip the image horizontally
    var planeMaterial = new THREE.MeshBasicMaterial({ transparent: true,
                                                      map: planeTexture });

    // Cloud background
    var cloudsTexture = new THREE.ImageUtils.loadTexture("./textures/clouds.jpg");
    cloudsTexture.wrapS = THREE.RepeatWrapping;
    cloudsTexture.wrapT = THREE.RepeatWrapping;
    var cloudsMaterial = new THREE.MeshBasicMaterial({ side: THREE.DoubleSide,
                                                       map: cloudsTexture });

    // House textures
    var house_ts = ["blue_panels", "chimney", "green_panels", "orange_panels", 
      "patio_wood", "patio_pillar", "pink_panels", "roof_shingles", 
      "yellow_panels", "yellow_shingles", "bottom", "bezier_3"];

    var textures = house_ts.map(function(t) { return "./textures/" + t + ".jpg" });
    materials = [];

    // Load the rest of the textures.
    TW.loadTextures(textures,
      function (textures) {
        for (var i = 0; i < textures.length; i++) {
          textures[i].wrapS = THREE.RepeatWrapping;
          textures[i].wrapT = THREE.RepeatWrapping;
          materials.push(new THREE.MeshPhongMaterial({ map: textures[i] }));
        }
        state = TW.cameraSetup(renderer, scene, UP.getBoundingBox());
        light(raysMaterial);
        draw(cloudsMaterial, planeMaterial);

        firstState();
        animate();
      }
    );
  }

  function draw(cloudsMaterial, planeMaterial) {
    upHouse = new UP.House(materials);
    upHouse.rotation.set(0, -Math.PI / 2, 0);
    scene.add(upHouse);

    background = new UP.Background(cloudsMaterial);
    background.position.set(0, 425, -70);
    background.rotation.set(0, 0, Math.PI / 2);
    scene.add(background);

    plane = new UP.Plane(planeMaterial);
    plane.position.set(-1000, 50, 0); // out of view initially
    scene.add(plane);
  }

  function light(raysMaterial) {
    var pa = LIGHTPARAMS.ambientLight;
    var ambient = new THREE.AmbientLight(pa.color);
    scene.add(ambient);

    var pd = LIGHTPARAMS.directionalLight;
    var directional = new THREE.DirectionalLight(pd.color, pd.intensity);
    directional.position.set(pd.position.x, pd.position.y, pd.position.z);
    scene.add(directional);

    var sunGeometry = new THREE.SphereGeometry(15);
    var sunMaterial = new THREE.MeshBasicMaterial({ color: 0xffff4f });
    var sun = new THREE.Mesh(sunGeometry, sunMaterial);
    sun.position.set(pd.position.x, pd.position.y, pd.position.z);
    scene.add(sun);

    var raysGeometry = new THREE.PlaneGeometry(175, 175, 0);
    var rays = new THREE.Mesh(raysGeometry, raysMaterial);
    rays.position.set(pd.position.x, pd.position.y, pd.position.z);
    scene.add(rays);
  }

  function resetAnimationState() {
    animationState = {
      house_positionY: 0,
      plane_positionX: -1000,
      lastParam: null
    };
  }

  function updateState() {
    // Update the house position
    animationState.house_positionY += 0.4;
    upHouse.position.y += 0.4;

    if (animationState.house_positionY > 600) {
      animationState.house_positionY = 0;
      upHouse.position.y = 0;
    }

    // Update the plane position
    animationState.plane_positionX += 2;
    plane.position.x += 2;

    if (animationState.plane_positionX > 600) {
      animationState.plane_positionX = -1000;
      plane.position.x = -1000;
    }
  }

  function firstState() {
    resetAnimationState();
    renderer.render(scene, state.cameraObject);
  }

  function oneStep() {
    updateState();
    renderer.render(scene, state.cameraObject);
  }

  function animate() {
    oneStep();
    animationID = requestAnimationFrame(animate);
  }

  function stopAnimation() {
    if (animationID != null) {
      cancelAnimationFrame(animationID);
      console.log("Cancelled animation using " + animationID);
    }
  }

  return {
    init: init
  };
})();