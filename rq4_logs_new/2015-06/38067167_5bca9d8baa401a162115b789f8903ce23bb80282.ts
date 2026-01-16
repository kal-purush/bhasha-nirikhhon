// MODULES

module ModuleWithExport {
  export class Hello {
    constructor() {
      var world = new World();
    }
  }

  class World {
    constructor() {
      console.log('world constructor');
    }
  }
}

class World {
  constructor() {
    console.log('outside world constructor');
  }
}

var world = new World();
var world2 = new ModuleWithExport.Hello();