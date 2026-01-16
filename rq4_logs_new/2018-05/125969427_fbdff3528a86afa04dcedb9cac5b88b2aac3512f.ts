export class Grades {
  constructor(public name: string, public value: number){}

  public getName(){
    return this.name;
  }

  public getValue(){
    return this.value;
  }
}