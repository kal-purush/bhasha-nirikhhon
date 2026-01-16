import { Component, OnInit } from '@angular/core';
import { GetLocationBody } from '../SystemAdministration/organization/GetLocationBody';
import { CountryService } from 'src/app/WebServices/AllWeb.service';
import { GetAllLocationResponse } from '../HRPayroll/employee/EmployeeApiResponse/GetAllLocationResponse';
import { INoRowsOverlayAngularComp } from 'ag-grid-angular';

@Component({
  selector: 'app-location-dropdown',
  templateUrl: './location-dropdown.component.html',
  styleUrls: ['./location-dropdown.component.css']
})
export class LocationDropdownComponent implements INoRowsOverlayAngularComp {
  agInit(params): void {
    this.getAllLocation();

  }

  locationResponse: GetAllLocationResponse[];
  constructor(private countryService: CountryService) {
  }

  getAllLocation(): any {
    var locationBody = new GetLocationBody();
    locationBody.userID = 1;
    this.countryService.doGetLocation(locationBody)
      .subscribe(
        data => {
          this.locationResponse = data;
          var getAllLocationResponse = new GetAllLocationResponse();
          getAllLocationResponse.name = 'Select';
          this.locationResponse[0] = getAllLocationResponse;
          console.log('data:  ' + JSON.stringify(this.locationResponse));
        }

      );
  }
}