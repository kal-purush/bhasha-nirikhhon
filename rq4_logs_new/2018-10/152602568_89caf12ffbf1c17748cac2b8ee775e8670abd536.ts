import { Rental } from './../shared/rental.model';
import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Params } from '@angular/router';
import { RentalService } from '../shared/rental.service';

@Component({
  selector: 'bwm-rental-detail',
  templateUrl: './rental-detail.component.html',
  styleUrls: ['./rental-detail.component.scss']
})
export class RentalDetailComponent implements OnInit {

  constructor(private _activatedRoute: ActivatedRoute, private _rentalService: RentalService) { }
  public rental: Rental;
  ngOnInit() {
    this._activatedRoute.params.subscribe((params: Params)=>{
      console.log(params)
      if(params['rentalID']){
        this._rentalService.getRentalDetail(params['rentalID']).subscribe(x=>this.rental=x);
      }
    })
  }

}