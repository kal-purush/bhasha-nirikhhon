import { Component, OnInit } from '@angular/core';
import { NovetiesService } from  '../../Services/noveties/noveties.service';


@Component({
  selector: 'app-noveties',
  templateUrl: './noveties.component.html',
  styleUrls: ['./noveties.component.css']
})
export class NovetiesComponent implements OnInit {

  noveties=[{'asd':'asd'}];
  
  constructor(private novetiesService: NovetiesService) {
  }

  ngOnInit() {
     this.novetiesService.getNoveties().subscribe(
     (resNovetiesData => this.noveties = resNovetiesData)
     );

    
  }

}