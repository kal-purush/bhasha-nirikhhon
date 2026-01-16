import { Component } from "@angular/core";
import { CloseTabService } from "../../../../core/services/event/close-tab.service";
import { ActivatedRoute, Router } from "@angular/router";

@Component({
  selector: 'purchase-order-list',
  templateUrl: './purchase-order-list.component.html',
  styleUrls: [ './purchase-order-list.component.less' ]
})
export class PurchaseOrderListComponent {

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