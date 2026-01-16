var juegoEstado = {
  // Aquí se inicializan las variables
  init: function () {
    console.log("función init");
    //this.scale.scaleMode = Phaser.ScaleManager.SHOW_ALL;
    this.scale.scaleMode = Phaser.ScaleManager.EXACT_FILL;
    this.scale.pageAlignHorizontally = true;
    this.scale.pageAlignVertically = true;
  },
  // Aquí se cargas las imágenes, audios, etc.
  preload: function () {
    console.log("función preload");
  },
  // Aquí se dibuja, se anima, etc.
  create: function () {
    console.log("función create");
  },
  // Esta función se ejecuta muchas veces
  update: function () {
    console.log("función update");
  }
};

var game = new Phaser.Game(722, 642, Phaser.AUTO);

// El primer parámetro es sólo una etiqueta y el segundo es
// el objeto de estados.
game.state.add("JuegoEstado", juegoEstado);


game.state.start("JuegoEstado");