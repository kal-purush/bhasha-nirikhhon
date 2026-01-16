import { Component, OnInit, Output, EventEmitter } from '@angular/core';

@Component({
  selector: 'app-mynav',
  templateUrl: './mynav.component.html',
  styleUrls: ['./mynav.component.scss']
})
export class MynavComponent implements OnInit {
  // @Output() onTitleClick = new EventEmitter<string>();  
  records1;
  records2;
  records3;
  // isShowListContent: boolean = true;
  isShowListContents = [true, true, true];

  constructor() { }

  ngOnInit() {
    this.records1 = ["Meet Now", "Schedule", "My Calendar", "Find Events"];
    this.records2 = ["My Usage"];
    this.records3 = ["Contact"];
  }
  onClick(value) {
    console.log("vvv" + value);
    this.isShowListContents[value] = !this.isShowListContents[value];
    // this.isShowListContent = !this.isShowListContent;
  }
  // onClick_TitleBar(value) {
  //   // console.log('onClick_HeaderBar', value);
  //   console.log("leiweiwww");
  //   this.isShowListContent = !this.isShowListContent;
  // }

}