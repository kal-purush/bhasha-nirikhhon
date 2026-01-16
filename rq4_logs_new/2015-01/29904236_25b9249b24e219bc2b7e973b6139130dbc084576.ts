const enum Keys {
  Left = 65, Up = 87, Right = 68, Down = 83
}

class Keyboard {
  keys: Object;

  constructor() {
    this.keys[Keys.Left] = false;
    this.keys[Keys.Up] = false;
    this.keys[Keys.Right] = false;
    this.keys[Keys.Down] = false;
    
    document.addEventListener('keydown', this.toggleKey.bind(this, true), false);
    document.addEventListener('keyup', this.toggleKey.bind(this, false), false);
  }

  toggleKey(state, e) {
    if (this.keys[e.keyCode] !== undefined) {
      this.keys[e.keyCode] = state;

      e.preventDefault && e.preventDefault();
      e.stopPropagation && e.stopPropagation();
    }
  }
}