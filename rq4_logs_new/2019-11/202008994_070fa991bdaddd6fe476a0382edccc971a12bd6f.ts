import { Component, OnInit } from '@angular/core';
import { FormBuilder, Validators } from '@angular/forms';
import { ActivatedRoute, Router } from '@angular/router';
import { PageRequest, Room, Penalty, Tenant } from '@gmrc/models';
import { RoomService, TenantService, PaymentEnumService, PaymentService, NotificationService } from '@gmrc/services';
import { FilterType } from '@gmrc/enums';

@Component({
  selector: 'app-penalty-form',
  templateUrl: './penalty-form.component.html',
  styleUrls: ['./penalty-form.component.scss']
})
export class PenaltyFormComponent implements OnInit {
  form = this.formBuilder.group({
    roomNumber: ['', Validators.required],
    date: ['', Validators.required],
    tenant: [''],
    tenantObjectId: ['', Validators.required],
    fine: ['', Validators.required],
    violation: ['', Validators.required],
    _id: null,
  });
  isLoading = true;
  isSubmitting = false;
  roomNumbers: Array<number> = [];
  pageRequest = new PageRequest(null, null);
  model: Penalty;
  buttonName = 'Add';
  formTitle = 'ADD PENALTY';
  tenants: Array<Tenant> = [];
  constructor(
    private formBuilder: FormBuilder,
    private route: ActivatedRoute,
    private roomService: RoomService,
    private tenantService: TenantService,
    private paymentEnumService: PaymentEnumService,
    private paymentService: PaymentService,
    private notificationService: NotificationService,
    private router: Router,
  ) { }

  ngOnInit() {
    this.isGoingToUpdate();
  }
  async getRoomNumbers(): Promise<void> {
    try {
      this.pageRequest.filters.type = FilterType.ALLROOMS;
      const rooms = await this.roomService.getRooms<Room>(this.pageRequest);
      rooms.data.forEach(room => {
        this.roomNumbers.push(room.number);
      });
    } catch (error) {}
  }
  loadFormValue(): void {
    this.form.patchValue({
      roomNumber: this.model.roomNumber,
      date: this.model.date,
      tenant: `${this.model.tenant[0].firstname} ${this.model.tenant[0].middlename} ${this.model.tenant[0].lastname}`,
      tenantObjectId: this.model.tenant[0]._id,
      violation: this.model.violation,
      fine: this.model.fine,
      _id: this.model._id,
    });
  }
  async getPenaltyByObjectId(objectId: string): Promise<void> {
    try {
      this.pageRequest.filters.type            = FilterType.PENALTYBYOBJECTID;
      this.pageRequest.filters.penaltyObjectId = objectId;
      const penalty                            = await this.paymentService.getPenalties<Penalty>(this.pageRequest);
      this.model                               = penalty.data[0];
      this.loadFormValue();
      this.buttonName                          = 'Update';
      this.formTitle                           = 'UPDATE PENALTY';
      this.isLoading                           = false;
    } catch (error) {
      console.log('the fucking error ', error);

    }
  }
  isGoingToUpdate(): void {
    const penaltyObjectId = this.route.snapshot.paramMap.get('id');
    this.getRoomNumbers();
    if (penaltyObjectId !== null) {
      this.getPenaltyByObjectId(penaltyObjectId);
    } else {
      this.isLoading = false;
    }
  }
  patchTenantObjectId(tenantObjectId: string): void {
    this.form.get('tenantObjectId').setValue(tenantObjectId);
  }
  async searchTenant(inputTenantName: string): Promise<void> {
    if (inputTenantName.length !== 0 ) {
      try {
        this.pageRequest.filters.tenantName = inputTenantName;
        this.pageRequest.filters.type       = FilterType.TENANTBYKEYSTROKE;
        const tenants                       = await this.tenantService.getTenants<Tenant>(this.pageRequest);
        this.tenants                        = tenants.data;
      } catch (error) {}
    }
  }
  async onSubmit(): Promise<void> {
    this.isSubmitting = true;
    try {
      const penalty             = this.model ? await this.paymentService.updatePenalty(this.form.value)
                                             : await this.paymentService.addPenalty(this.form.value);
      // tslint:disable-next-line: max-line-length
      const notificationMessage = this.model ? `Updated penalty for room number ${penalty.roomNumber}`
                                             // tslint:disable-next-line: max-line-length
                                             : `Added penalty for room number ${penalty.roomNumber}`;
      this.form.get('_id').setValue(penalty._id);
      this.model                = penalty;
      this.buttonName           = 'Update';
      this.notificationService.notifySuccess(notificationMessage);
      this.isSubmitting         = false;
    } catch (error) {
      this.notificationService.notifyFailed('Something went wrong');
      this.isSubmitting = false;
    }
  }
  routeToPenalties(): void {
    this.router.navigate(['payment/penalty']);
  }
}