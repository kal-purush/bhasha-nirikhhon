/*/// <reference path="../typings/threejs/three.d.ts"/>*/

module NineTails {
  export class Color {
      private handlers;
      red: number;
      green: number;
      blue: number;

      constructor(red: number, green: number, blue: number) {
          this.red = red;
          this.green = green;
          this.blue = blue;

          this.handlers = [];
      }

      get() : string {
          return 'rgb(' + this.red + ', ' + this.green + ', ' + this.blue + ')';
      }

      set(red : number, green : number, blue : number) : void {
        this.red = red;
        this.green = green;
        this.blue = blue;

        for(var i = 0; i < this.handlers.length; i++) {
          this.handlers[i].handler.call(this.handlers[i].context, { newValue: this.get() }, this.handlers[i].extra);
        }
      }

      onSet(handler, context, extra) : void {
        this.handlers.push({ handler: handler, context: context, extra: extra });
      }
  }
}