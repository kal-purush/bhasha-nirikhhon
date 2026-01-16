import { Component, OnInit } from "@angular/core";
import { ObservableArray } from "tns-core-modules/data/observable-array/observable-array";

import { Bill } from "./shared/bill.model";
import { BillService } from "./shared/bill.service";

import { Kinvey } from "kinvey-nativescript-sdk";
import { UtilService } from "../shared/utils.service";

import { RouterExtensions } from "nativescript-angular/router";

@Component({
  selector: "BillList",
  moduleId: module.id,
  templateUrl: "./bill-list.component.html",
  styleUrls: ["./bill-list.component.scss"]
})
export class BillListComponent implements OnInit {
  private _isLoading: boolean = false;
  private _bills: ObservableArray<Bill> = new ObservableArray<Bill>([]);

  constructor(
    private _billService: BillService,
    private _routerExtensions: RouterExtensions
  ) { }

  ngOnInit(): void {
    this._isLoading = true;

    this._billService.loadUserBills(Kinvey.User.getActiveUser()._id)
    .then((bills: Array<Bill>) => {
      this._bills = new ObservableArray(bills);
      this._bills.forEach((bill) => {
        bill.colorClass = UtilService.generateRandomTileColor();
      });
      this._isLoading = false;
    }).catch(() => {
      this._isLoading = false;
    });
  }

  get bills(): ObservableArray<Bill> {
    return this._bills;
  }

  get isLoading(): boolean {
    return this._isLoading;
  }

  editBill(billdId: string): void {
    this._routerExtensions.navigate(["/bills/bills-edit", billdId],
      {
        animated: true,
        transition: {
          name: "slide",
          duration: 200,
          curve: "ease"
        }
      });
  }

  newBillScreen() {
    this._routerExtensions.navigate(["/bills/new-bill"],
      {
        animated: true,
        transition: {
          name: "slide",
          duration: 200,
          curve: "ease"
        }
      });
  }
}