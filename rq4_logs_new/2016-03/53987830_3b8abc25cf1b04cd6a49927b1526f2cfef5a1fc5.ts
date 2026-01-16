import {Component, Input} from 'angular2/core';
import ReadingService from './ReadingService';
import GraphComp from './GraphComp';

import * as moment from 'moment';
import * as _ from 'lodash';

@Component({
  selector: 'graph-paginator',
  directives: [GraphComp],
  template: `
    <div>
      <div>
        <span>Current:</span>&nbsp;
        <strong>{{currentRangeAsDate.start | date}}</strong> &mdash; <strong>{{currentRangeAsDate.end | date}}</strong>
      </div>
      <div>
        <div style='float: left; padding-right: 15px;'>{{range.start|date}}</div>
        <div class="slider" style="width: 400px; float: left"></div>
        <div style='float: left; padding-left: 15px;'>{{range.end|date}}</div>
        <br style='clear: both'>
      </div>
      <div>
      <graph-graph [data]="data"></graph-graph>
      </div>
    </div>
  `
})
export default class GraphPaginator{
  data=[];
  currentRange:any={};
  currentRangeAsDate={};
  @Input() patient;
  @Input() week=1;
  range:any={};
  slider:any;
    
  constructor(public Readings: ReadingService){
    //this.Readings.dataStream.skip(1).subscribe(this.readingsChanged.bind(this));
  }
  
  ngAfterContentInit(){
    this.Readings.dateRange()
    .then((range)=>{
      let initialRange=[Math.max(moment(range.start), moment(range.end).subtract(this.week, 'weeks')), range.end.getTime()];
      this.range=range;
      this.slider=$(".slider").slider({
        range: true,
        min: range.start.getTime(),
        max: range.end.getTime(),
        values: initialRange,
        stop: this.sliderChanged.bind(this),
        step: 1000*60*60*24 //1 nap
      });
      this.sliderChanged();
    });
    
  }
  
  sliderChanged(event, ui){
    let range=this.slider.slider("values");
    _.assign(this.currentRange, {
      start: range[0],
      end: range[1]
    });
    _.assign(this.currentRangeAsDate, {
      start: new Date(range[0]),
      end: new Date(range[1])
    });
    
    this.Readings.queryRange(new Date(range[0]), new Date(range[1]))
    .then(readings=>{
      this.data=readings;
    });
  }
}