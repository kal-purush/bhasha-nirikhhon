class Vehicle {
  // 3 different ways to define a local fields in a class
  color: string; // 1
  year = 2000; // 2

  constructor(color: string, public maker: string /*3*/) {
    this.color = color;
  }

  drive(): void {
    console.log('chugga chugga');
  }

  honk(): void {
    console.log('Beep');
  }
}

const vehicle = new Vehicle('red', 'IKE');
vehicle.drive();
vehicle.honk();

console.log(vehicle.color);
console.log(vehicle.year);
console.log(vehicle.maker);

class Automobile extends Vehicle {
  constructor(public wheels: number, color: string, maker: string) {
    super(color, maker); // because of parent class needs to execute the constructor
  }

  drive(): void {
    console.log('Override chugga chugga');
  }
}

const automobile = new Automobile(4, 'red', 'IKE');
automobile.drive();
automobile.honk();