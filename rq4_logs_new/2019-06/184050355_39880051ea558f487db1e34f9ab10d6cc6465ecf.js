class Vec{
  constructor(x, y){
  this.x = x;
  this.y = y;
  }
  plus(vec2){
  return new Vec(this.x + vec2.x, this.y + vec2.y);
  }
  minus(vec2){
  return new Vec(this.x - vec2.x, this.y - vec2.y);
  }
  get length(){
  return Math.sqrt(this.x * this.x + this.y * this.y);
  }
}
console.log(new Vec(1, 2).plus(new Vec(2, 3)));
console.log(new Vec(1, 2).minus(new Vec(2, 3)));
console.log(new Vec(3, 4).length);