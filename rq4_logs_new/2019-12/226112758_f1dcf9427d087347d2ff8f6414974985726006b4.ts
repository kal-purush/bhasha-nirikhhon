import { Component } from "@angular/core";
import { CloseTabService } from "../../../../core/services/event/close-tab.service";
import { Router } from "@angular/router";

@Component({
  selector: 'purchase-order-info',
  templateUrl: './purchase-order-info.component.html',
  styleUrls: [ './purchase-order-info.component.less' ]
})
export class PurchaseOrderInfoComponent {

  constructor(
    private closeTabService: CloseTabService,
    private router: Router
  ) {

  }

  test(): void {
    console.log(this.router.url);
    this.closeTabService.event.emit({
      url: this.router.url,
      refreshUrl: null
    });
  }

}