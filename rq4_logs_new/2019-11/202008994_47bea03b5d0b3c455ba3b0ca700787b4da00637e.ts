import { Component, OnInit } from '@angular/core';
import { FormBuilder } from '@angular/forms';
import { PageRequest, Room } from '@gmrc/models';
import { RoomService, PaymentEnumService } from '@gmrc/services';
import { FilterType } from '@gmrc/enums';
import * as moment from 'moment';
import { Moment} from 'moment';
import { MatDatepicker } from '@angular/material';

@Component({
  selector: 'app-room-payment-advance-search',
  templateUrl: './room-payment-advance-search.component.html',
  styleUrls: ['./room-payment-advance-search.component.scss']
})
export class RoomPaymentAdvanceSearchComponent implements OnInit {
  roomNumbers: number[] = [];
  searchFiltersForm = this.formBuilder.group({
    roomNumber: null,
    date: null,
    electricBillStatus: null,
    waterBillStatus: null,
    riceCookerBillStatus: null,
    rentStatus: null,
  });

  pageRequest = new PageRequest(null, null);
  date: Moment;
  constructor(
    private formBuilder: FormBuilder,
    private roomService: RoomService,
    private paymentEnumService: PaymentEnumService
  ) { }

  ngOnInit(): void {
    this.getRoomNumbers();
  }
  getRoomNumbers(): void {
    this.pageRequest.filters.type = FilterType.ALLROOMS;
    this.roomService.getRooms<Room>(this.pageRequest).then( rooms => {
      rooms.data.forEach(room => {
        this.roomNumbers.push(room.number);
      });
    })
    .catch( err => {});
  }
  get monthYear(): string {
    if (!this.date) { return null; }
    return moment(this.date).format('MM/YYYY');
  }
  chosenMonthHandler(date: Moment, datepicker: MatDatepicker<Moment>): void {
    this.date = date;
    this.searchFiltersForm.get('date').setValue(this.monthYear);
    datepicker.close();
  }

}