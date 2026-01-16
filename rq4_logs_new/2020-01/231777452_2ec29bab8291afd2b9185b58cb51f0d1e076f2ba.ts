import { Component, OnInit } from '@angular/core';
import { Property } from '../../../models/property';
import { PropertiesService } from '../services/properties.service';
import { Router } from '@angular/router';

@Component({
    templateUrl: './properties.component.html'
})

export class PropertiesComponent implements OnInit {
  constructor(
    private propertiesService: PropertiesService,
    private router: Router
  ) { };

  properties: Array<Property> = new Array<Property>();
  pageTitle: string = "Lista dostępnych nieruchomości";
  tempInfo: string = "Loading...";
  ngOnInit(): void{
    this.downloadProperties();
  }
  downloadProperties(): void {
   
    this.propertiesService.getProperties().subscribe(
      propertiesFromDB => {
        if (propertiesFromDB.length == 0) {
          this.tempInfo = "Records not found";
        }
        else {
          this.properties = propertiesFromDB;
        }
      }),
        error => console.log(error)
  }
  getProperty(id: number): void {
    this.router.navigate(['./properties/property-details', id]);
    }

    updateProperty(id: number): void {
      this.router.navigate(['./properties/property-update', id]);
  }
  deleteProperty(id: number): void {
    this.propertiesService.deleteProperty(id).subscribe(
      onSuccess => console.log(onSuccess),
      onError => console.log(onError)
    );
    this.properties.splice(this.properties.findIndex(prop => prop.Id == id), 1)
  }

  }
 
