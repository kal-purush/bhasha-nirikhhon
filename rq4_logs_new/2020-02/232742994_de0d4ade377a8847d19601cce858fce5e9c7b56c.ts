import { Component, OnInit, ElementRef, Input } from '@angular/core';
import { NbDialogService } from '@nebular/theme';
import { VehicleService } from '../../vehicles/vehicles.service';
import { VehicleservService } from '../vehicleserv.service';

import { ActivatedRoute, Router } from '@angular/router';


@Component({
  selector: 'ngx-vehicle-detail-s',
  templateUrl: './vehicle-detail-s.component.html',
  styleUrls: ['./vehicle-detail-s.component.scss']
})
export class VehicleDetailSComponent implements OnInit {
  public vehicleDetails = [];
  constructor(private vehicleService: VehicleService, private vehicleservService: VehicleservService, private activeRoute: ActivatedRoute, private eRef: ElementRef, private router:Router, private dialogService: NbDialogService) { }
  @Input() vehicleId:String;
  ngOnInit() {
    this.loadVehicles(this.vehicleId);
  }

  public loadVehicles(vehicleId){
    this.vehicleService.loadVehicle(vehicleId).subscribe((vehicleData:any)=>{
      // console.log(vehicleData);
     this.vehicleDetails = vehicleData.data[0];
     console.log(this.vehicleDetails);
    },(error)=>{

    });
  }

  open(dialog:any) {
    this.dialogService.open(dialog, { context: 'this is some additional data passed to dialog' });
  }

 

}