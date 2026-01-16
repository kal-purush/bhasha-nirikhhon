import { Component, OnInit } from '@angular/core';
import {Title} from '@angular/platform-browser';

@Component({
  selector: 'app-drawer-menu',
  templateUrl: './drawer-menu.component.html',
  styleUrls: ['./drawer-menu.component.scss']
})
export class DrawerMenuComponent implements OnInit {
  status: boolean = false;
  constructor(private title: Title) {
    title.setTitle('Drawer Menu');
  }

  ngOnInit(): void {
  }

  // document.querySelector('.toggle').onclick = function(){
  //   this.classList.toggle('active')
  // }

  clickEvent(){
    this.status = !this.status;
    console.log(this.status, 'this.status');
  }
}