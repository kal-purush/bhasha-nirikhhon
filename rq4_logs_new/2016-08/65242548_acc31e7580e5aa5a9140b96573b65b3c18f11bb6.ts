export class CheckChangedEvent{
  group:number;
  index:number;
  value:boolean;
  constructor(group:number,index:number, value:boolean){
    this.group = group;
    this.index = index;
    this.value = value;
  }
}