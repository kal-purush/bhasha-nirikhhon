import { Component, OnInit, Input, ChangeDetectorRef } from '@angular/core';
import { MapService } from './map.service';

@Component({
  selector: 'br-map',
  templateUrl: './map.component.html',
  styleUrls: ['./map.component.scss']
})
export class MapComponent implements OnInit {

  @Input() location : string;
  isPositionError: boolean = false;
  
  lat: number;
  lng: number;

  constructor(private mapService: MapService, private ref: ChangeDetectorRef) { }

  ngOnInit() {
  }

  mapReadyHandler(){
    // if(Math.round(Math.random() *10) > 5){

    // }
    this.mapService.geocodeLocation(this.location).subscribe(
      (coordinates) => {
        this.lat = coordinates.lat;
        this.lng = coordinates.lng;

        this.ref.detectChanges();
      }, () => {
        this.isPositionError = true;
      }
    );
  }

}