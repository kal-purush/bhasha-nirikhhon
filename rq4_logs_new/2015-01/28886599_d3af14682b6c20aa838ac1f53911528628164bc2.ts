/// <reference path="definitions/phaser.d.ts"/>

module SloubyVolley {
  export class Game extends Phaser.Game {
    constructor() {
      super(800, 600, Phaser.CANVAS, '-slouby-volley');

      this.state.add('boot', State.Boot);
      this.state.add('preload', State.Preload);
      this.state.add('main', State.Main);

      this.state.start('boot');
    }
  }
}