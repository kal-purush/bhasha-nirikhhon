import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';
import { Location } from '@angular/common';
import { Charity } from '../../constants/data-types';
import { CharityService } from '../../services/charity.service';

@Component({
  selector: 'charity-detail',
  templateUrl: './charity-detail.component.html',
  styleUrls: ['./charity-detail.component.css']
})

export class CharityDetailComponent implements OnInit {
  charity:Charity;
  
  constructor(private charityService:CharityService,
              private route:ActivatedRoute,
              private location:Location) {
  }
  
  ngOnInit():void {
    this.route.params.forEach((params:Params) => {
      let id = +params['id'];
      this.charityService.getCharity(id)
        .then(charity => this.charity = charity);
    });
  }
  
  // save():void {
  //   this.heroService.update(this.hero)
  //     .then(() => this.goBack());
  // }
  //
  // goBack():void {
  //   this.location.back();
  // }
}