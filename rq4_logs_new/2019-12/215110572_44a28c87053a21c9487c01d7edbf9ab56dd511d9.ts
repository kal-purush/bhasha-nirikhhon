import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-restaurants-list',
  templateUrl: './restaurants-list.component.html',
  styleUrls: ['./restaurants-list.component.scss']
})
export class RestaurantsListComponent implements OnInit {

  restaurants = [
    {
      id: 1,
      name: 'Restaurant_1',
      address: 'ul.Grunwaldzka 1',
      active_lobby: 2,
    },
    {
      id: 2,
      name: 'Restaurant_2',
      address: 'ul.Grunwaldzka 2',
      active_lobby: 5,
    },
    {
      id: 3,
      name: 'Restaurant_3',
      address: 'ul.Grunwaldzka 3',
      active_lobby: 0,
    },
    {
      id: 4,
      name: 'Restaurant_4',
      address: 'ul.Grunwaldzka 4',
      active_lobby: 1,
    },
    {
      id: 5,
      name: 'Restaurant_5',
      address: 'ul.Grunwaldzka 5',
      active_lobby: 3,
    }];

  constructor() { }

  ngOnInit() {
  }

}