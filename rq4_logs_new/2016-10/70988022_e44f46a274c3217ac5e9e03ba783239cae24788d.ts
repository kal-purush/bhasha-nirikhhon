import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { Charity } from '../../constants/data-types';
import { CharityService } from '../../services/charity.service';

@Component({
  selector: 'charities',
  templateUrl: '/charities.component.html',
  styleUrls: ['./charities.component.css']
})


export class CharitiesComponent implements OnInit {
  charities:Charity[] = [];
  
  constructor(private router:Router,
              private charityService:CharityService) {
  }
  
  ngOnInit():void {
    this.charityService.getCharities()
      .then(charities => this.charities = charities);
  }
  
  // gotoDetail(hero:Hero):void {
  //   let link = ['/detail', hero.id];
  //   this.router.navigate(link);
  // }
}