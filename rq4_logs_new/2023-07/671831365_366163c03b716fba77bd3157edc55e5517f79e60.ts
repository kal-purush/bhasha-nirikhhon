import { Component, OnInit } from '@angular/core';
import { UserData } from 'src/app/shared/models/user_data';

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.css']
})
export class HomeComponent implements OnInit {

  user_datas: UserData[] = [];

  constructor() { 
  }

  ngOnInit(): void {
  }

}