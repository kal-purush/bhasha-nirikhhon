
class Animal {
  name : string;
  legs : number;
  lifeExpectancy : number;
}

class Dog {
  tail : boolean;
}

class Cat {
  tail : boolean;
}

class Fish extends Animal {
  gills : boolean;
}

//-----------------

class Context {
  dog  = new Dog();
  cat  = new Cat();
  fish = new Fish();
}


//-----------------

interface Processor<T extends Animal> {
  processDependencies();
  process(...animals : T[]);
}

module Western {

  interface Dependencies {
    fish;
  }

  export class FoodProcessor implements Processor<Fish> {
    constructor(private di : Dependencies) {
    }

    process(...fishes : Fish[]) {
      console.log(`Some tasty ${ fishes.map(fish => fish.name) } tartar! Haaarr...`);
    }

    processDependencies() {
      console.log(`Some tasty ${this.di.fish} tartar! Haaarr...`);
    }
  }

}


module Chinese {
  interface Dependencies {
    dog;
    cat;
  }

  export class FoodProcessor implements Processor<Animal> {
    constructor(private di : Dependencies) { }

    process(...animals : Animal[]) {
      console.log(`${ !!animals && animals.map(a => a.name)  } 个饱!`);
    }

    processDependencies() {
      console.log(`${this.di.dog} 和 ${this.di.cat} 个饱!`);
    }
  }

}


//--------------------

var context = new Context();
new Western.FoodProcessor(context);
new Chinese.FoodProcessor(context);