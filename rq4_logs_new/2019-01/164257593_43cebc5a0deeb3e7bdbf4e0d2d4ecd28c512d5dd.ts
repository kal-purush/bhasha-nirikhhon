import { Component, OnInit } from '@angular/core';
import { transaction } from '../transactions';
import { AssetService } from '../asset.service';

import { Location } from '@angular/common';

@Component({
  selector: 'app-transaction',
  templateUrl: './transaction.component.html',
  styleUrls: ['./transaction.component.css']
})
export class TransactionComponent  implements OnInit{

  Asset = new transaction();
  submitted = false;

  ngOnInit()
  {
    this.Asset.transaction = true;
    document.getElementById("symbol").style.background="rgb(76, 243, 76)";
    document.getElementById("shares").style.background="rgb(76, 243, 76)";
    document.getElementById("price").style.background="rgb(76, 243, 76)";
    document.getElementById("buydate").style.background="rgb(76, 243, 76)";
    document.getElementById("symbol").style.color="white";
    document.getElementById("shares").style.color="white";
    document.getElementById("price").style.color="white";
    document.getElementById("buydate").style.color="white";
  }

  constructor(
    private assetService: AssetService,
    private location: Location,
  ) { }

  setTransaction(value: boolean): void {
    alert("transaction is set to " + value);
    this.Asset.transaction = value;

    if ( value === true )
    {
      document.getElementById("symbol").style.background="rgb(76, 243, 76)";
      document.getElementById("shares").style.background="rgb(76, 243, 76)";
      document.getElementById("price").style.background="rgb(76, 243, 76)";
      document.getElementById("buydate").style.background="rgb(76, 243, 76)";
    }
    else
    {
      document.getElementById("symbol").style.background="rgb(253, 65, 65)";
      document.getElementById("shares").style.background="rgb(253, 65, 65)";
      document.getElementById("price").style.background="rgb(253, 65, 65)";
      document.getElementById("buydate").style.background="rgb(253, 65, 65)";
    }
  }
  newAsset(): void {
    this.submitted = false;
    this.Asset = new transaction();
  }

 addAsset() {
   this.submitted = true;
   this.save();
 }

  goBack(): void {
    this.location.back();
  }

  private save(): void {
    console.log(this.Asset);
    this.assetService.addAsset(this.Asset)
        .subscribe();
  }

}