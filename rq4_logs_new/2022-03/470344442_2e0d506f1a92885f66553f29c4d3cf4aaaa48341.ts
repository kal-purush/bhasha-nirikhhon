import { Component, OnInit, ViewChild } from '@angular/core';
import { Router } from '@angular/router';

// import { MatMenuTrigger } from '@angular/material/menu/matMenu';


@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.scss']
})


export class HeaderComponent implements OnInit {
  // @ViewChild(MatMenuTrigger) trigger: MatMenuTrigger;
  // someMethod() {
  //   this.trigger.openMenu();
  // }
  constructor(private router: Router) { }

  ngOnInit(): void {
  }
  menu() {
    this.router.navigate(['menu'])
  }

}