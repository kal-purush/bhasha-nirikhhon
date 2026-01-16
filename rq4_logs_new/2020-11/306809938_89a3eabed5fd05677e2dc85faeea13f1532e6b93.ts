import {Component, OnInit} from '@angular/core';
import {ParkingLotService} from '../service/parking-lot.service';
import {ParkingLot} from '../entity/parking-lot';
import {Floor} from '../entity/floor';

@Component({
  selector: 'app-list-parking-lot',
  templateUrl: './list-parking-lot.component.html',
  styleUrls: ['./list-parking-lot.component.css']
})
export class ListParkingLotComponent implements OnInit {
  listParkingLotApi: ParkingLot[] = [];
  listParkingLotShow: ParkingLot[] = [];
  currentPage: number;
  totalItem: number;
  floorList: Floor[];

  constructor(private parkingLotService: ParkingLotService) {
  }

  ngOnInit(): void {
    this.parkingLotService.getAllParkingLot().subscribe(
      list => {
        this.listParkingLotApi = list;
        this.totalItem = list.length;
        this.listParkingLotShow = list;
      },
      e => console.log(e)
    );

    this.parkingLotService.getAllFloor().subscribe(
      list => this.floorList = list
    );
  }

  filterByFloor(floor: string): void {
    if (floor === 'none') {
      this.listParkingLotShow = this.listParkingLotApi;
      return;
    }

    this.listParkingLotShow = this.listParkingLotApi.filter(par => {
      if (par.nameFloor === floor) {
        return par;
      }
    });
  }
}