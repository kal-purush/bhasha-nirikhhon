import { Component, OnInit } from '@angular/core';
import { ActivatedRoute} from '@angular/router';

import { Rental } from '../shared/rental.model';
import { RentalService } from '../shared/rental.service';

@Component({
  selector: 'bwm-rental-detail',
  templateUrl: './rental-detail.component.html',
  styleUrls: ['./rental-detail.component.scss']
})
export class RentalDetailComponent implements OnInit {
    CurrentId:string;
    rental:Rental;
  constructor(private route:ActivatedRoute,
    private rentalService:RentalService) { }

  ngOnInit() {
    this.route.params.subscribe(
      (params)=>{
        // this.CurrentId=params['rentalId'];
        this.getRental(params['rentalId']);
      }
    )

   
  }

   getRental(rentalID:String){
      this.rentalService.getRentalById(rentalID).subscribe(
        (rental:Rental)=>{
          return this.rental=rental;
      })
    }

}