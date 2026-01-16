import { Component, OnInit } from '@angular/core';
import { BeverageService } from '../beverage.service';
import { Router } from '@angular/router';

@Component({
  selector: 'beverage-create',
  templateUrl: './beverage-create.component.html',
  styleUrls: ['./beverage-create.component.css']
})

export class BeverageCreateComponent implements OnInit {

  beverage: Object = {};

  constructor() { }

  ngOnInit() {

  }

  createBeverage(beverage: Object) {
    
  }

}