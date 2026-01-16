export class Gun {
  name: string;
  price: number;
  accuracy: number;
  power: number;
  ammo: number;

  constructor(name: string, price: number, accuracy: number,
    power: number, ammo: number) {
    this.name = name;
    this.price = price;
    this.accuracy = accuracy;
    this.power = power;
    this.ammo = ammo;
  }
}