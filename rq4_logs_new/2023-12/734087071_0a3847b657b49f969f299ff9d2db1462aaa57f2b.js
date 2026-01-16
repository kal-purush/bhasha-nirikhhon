let patos, paredinhas;

function preload() {
  img = loadImage('duck01.jpg');
}

function setup() {
  createCanvas(400, 400);
  // allSprites.debug = true;

  world.gravity.y = 10;

  patos = new Group();
  patos.y = -40;
  patos.bounciness = 0.8;
  // patos.addAni(img);
  // patos.ani.scale = 0.3;

  paredinhas = new Group();
  paredinhas.collider = "k";
  // paredinhas.visible = false;
  new paredinhas.Sprite(200, 400, 400, 10);
  new paredinhas.Sprite(0, 200, 10, 400);
  new paredinhas.Sprite(400, 200, 10, 400);
}

function draw() {
  background("orchid");
  if (kb.pressing(' ')) {
    let p = new patos.Sprite()
    p.radius = random(5, 40);
    p.x = random(40, 360);
  }
  for (let i = 0; i < patos.length; i++) {
    const pato = patos[i];
    if (pato.mouse.pressed()) {
      pato.life = -1;
    }
  }
}
