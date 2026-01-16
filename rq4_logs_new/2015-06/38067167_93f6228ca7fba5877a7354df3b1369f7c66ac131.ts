// INTERFACES EXERCISES

// 01. b) interface for 01.a
interface IPlant {
  genus:string;
  species:string;
  name:string;
  // an interface is the minimal contract that has to be fulfilled
  // a class that implements an interface can have more than the interface provides
}

// 01. a) create an interface for this class
class OrchardApple implements IPlant {
  genus:string = 'Malus';
  species:string = 'Domestica';
  region:string = 'Northern Hemisphere';
  name:string = "Orchard Apple";
}

class Dandelion implements IPlant {
  genus:string = 'Taraxacum';
  species:string = 'Taraxacum';
  name:string = "Dandelion";
}

var orchardApple = new OrchardApple();
var dandelion = new Dandelion();