import { Component, OnInit } from '@angular/core';
import { FormBuilder } from '@angular/forms';
import { RoomEnumService, RoomService, TenantService } from '@gmrc/services';
import { PageRequest, Tenant, Room } from '@gmrc/models';
import { FilterType } from '@gmrc/enums';

@Component({
  selector: 'app-unsettle-bill-advance-search',
  templateUrl: './unsettle-bill-advance-search.component.html',
  styleUrls: ['./unsettle-bill-advance-search.component.scss']
})
export class UnsettleBillAdvanceSearchComponent implements OnInit {
  searchFiltersForm = this.formBuilder.group({
    roomType: null,
    roomNumber: null,
    tenant: null,
    tenantObjectId: null,
  });
  roomNumbers: Array<number> = [];
  tenants: Array<Tenant> = [];
  pageRequest = new PageRequest(null, null);
  constructor(
    private formBuilder: FormBuilder,
    private roomEnumService: RoomEnumService,
    private roomService: RoomService,
    private tenantService: TenantService
  ) { }

  ngOnInit() {
    this.getRoomNumbers();
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
  patchTenantObjectId(tenantObjectId: string): void {
    this.searchFiltersForm.get('tenantObjectId').setValue(tenantObjectId);
  }

}