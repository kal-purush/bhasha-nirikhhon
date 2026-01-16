import { Workshop } from '../shared/workshop.model';
import { WorkshopService } from '../shared/workshop.service';
import { Component, OnInit } from '@angular/core';
import {ActivatedRoute} from '@angular/router';


@Component({
  selector: 'ago-workshop-detail',
  templateUrl: './workshop-detail.component.html',
  styleUrls: ['./workshop-detail.component.scss']
})
export class WorkshopDetailComponent implements OnInit {

  workshop: Workshop;
  
  constructor(private route: ActivatedRoute,
              private workshopService: WorkshopService) { }

  ngOnInit() {
    this.route.params.subscribe(
      (params) => {
        this.getWorkshop(params ['workshopId']);
      }
    )
  }

  getWorkshop(workshopId: string){
    this.workshopService.getWorkshopById(workshopId).subscribe(
      (workshop: Workshop) => {
        this.workshop = workshop;
      }
    );
  }

}