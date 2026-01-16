import {Component} from 'angular2/core';
import GraphComp from './GraphComp';
import FormComp from './FormComp';
import ReadingService from './ReadingService';

@Component({
  selector: 'graph-app',
  directives: [GraphComp, FormComp],
  template: `<div>
    <div>
      <graph-graph [data]="d"></graph-graph>
    </div>
    <div>
      <graph-form></graph-form>
    </div>
  </div>`
})
export default class AppComp{
  d=[];
  constructor(public readingService:ReadingService){
    console.log("service(app)");
    console.log(readingService.dataStream.getValue());
    this.readingService.dataStream.subscribe(this.readingsChanged.bind(this));
  }
  
  readingsChanged(newReadings){
    console.log("readings changed from app");
    this.d=newReadings;
  }
  
  ngOnInit(){
    setTimeout(()=>{
      this.d=[...this.readingService.load()];
      console.log("loaded");
    }, 3000);
  }
}