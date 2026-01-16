import { Component, OnInit } from '@angular/core';
import { Driver } from 'src/app/models/Driver';
import { DriversService } from 'src/app/services/drivers.service';

@Component({
  selector: 'list-driver',
  templateUrl: './list.component.html',
  styleUrls: ['./list.component.css']
})
export class ListComponent implements OnInit {

  constructor(private driversService: DriversService) { }
  listDrivers: any = [];
  ngOnInit(): void {
    this.driversService.getDrivers().subscribe(
      res => {
        this.listDrivers = res;
      },
      err => console.log(err)
    );
  }

}