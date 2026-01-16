import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-child',
  templateUrl: './child.component.html',
  styleUrls: ['./child.component.css']
})
export class ChildComponent implements OnInit {
@Input() heroDetails;
@Input('dir') dirName: string;
@Input() major: number;
@Input() minor: number;


  constructor() {
    console.log('major',this.major,this.minor)
   }

  ngOnInit() {
  }
}