// app.js

class App {
  constructor(width, height) {
    this.walker = new Walker(width / 2, height / 2)

    // bind this to class methods
    this.update = this.update.bind(this)
    this.draw = this.draw.bind(this)
  }

  update(tick) {
    this.walker.update(tick)
  }

  draw(ctx, tick) {
    this.walker.draw(ctx, tick)
  }
}