import { Component, OnInit } from '@angular/core';
import { DataService, Kard } from '../shared/data.service';

@Component({
  moduleId: module.id,
  selector: 'sd-curator'
//  templateUrl: 'curator.component.html',
//  styleUrls: ['curator.component.css']
})
export class VisionComponent implements OnInit {
  d: DataService = null;
  constructor(public dataService: DataService) {
    this.d = dataService;
  }

  getUrl(){
    let url : string = (<HTMLInputElement>document.getElementById("url")).value;
    this.get_results(url);
  }

  get_results(url:String){
    let result = this.d.getVisionResults(url)
  }

  ngOnInit() {
  }

 // follow(val:boolean) {
 //   this.dataService.following[this.c.id] = val;
 // }

}