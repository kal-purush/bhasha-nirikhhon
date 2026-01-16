import { Component, OnInit } from '@angular/core';
import { PaymentService, NotificationService } from '@gmrc/services';
import { Route, ActivatedRoute } from '@angular/router';
import { PageRequest, RoomTenant, RoomPayment } from '@gmrc/models';
import { FilterType, PaymentStatus } from '@gmrc/enums';
import { MatTableDataSource } from '@angular/material';

@Component({
  selector: 'app-tenants',
  templateUrl: './tenants.component.html',
  styleUrls: ['./tenants.component.scss']
})
export class TenantsComponent implements OnInit {
  pageRequest = new PageRequest(null, null);
  isLoading = true;
  warningMessage: string = null;
  dataSource = new MatTableDataSource<RoomTenant>();
  displayedColumns: string[] = [
    'tenantName',
    'rent',
    'dueRentDate',
    'roomNumber',
    'status',
  ];

  constructor(
    private paymentService: PaymentService,
    private route: ActivatedRoute,
    private notificationService: NotificationService
  ) { }

  ngOnInit() {
    this.getRoomPayments();
  }
  getUnpaidBalanceTenantsRent(roomPayments: Array<RoomPayment>): void {
    const tenants: Array<RoomTenant> = [];
    roomPayments.forEach( roomPayment => {
      roomPayment.roomTenants.forEach( roomTenant => {
        const rentStatus = roomTenant.rentStatus.value;
        if ( rentStatus === PaymentStatus.UNPAID || rentStatus === PaymentStatus.BALANCE) {
          roomTenant.roomNumber = roomPayment.roomNumber;
          tenants.push(roomTenant);
        }
      });
    });
    this.dataSource.data = tenants;
    console.log(this.dataSource.data);
  }
  get date(): string {
    return this.route.snapshot.paramMap.get('date').replace('-', '/');
  }
  async getRoomPayments(): Promise<void> {
    this.pageRequest.filters.type = FilterType.ROOMPAYMENTSBYDATE;
    this.pageRequest.filters.date = this.date;
    try {
      const roomPayments = await this.paymentService.getRoomPayments<RoomPayment>(this.pageRequest);
      if (roomPayments.data.length === 0 ) {
        this.warningMessage = `No room payments found for ${this.date}`;
      } else {
        this.getUnpaidBalanceTenantsRent(roomPayments.data);
      }
      this.isLoading = false;
    } catch (error) {
      this.notificationService.notifyFailed('Something went wrong');
      this.isLoading = false;
    }
  }
}