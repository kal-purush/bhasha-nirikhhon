import { Component, OnInit } from '@angular/core';
import { currentAssets } from '../currentAssets';
import { AssetService } from '../asset.service';

import { Location } from '@angular/common';

@Component({
  selector: 'app-add-asset',
  templateUrl: './add-asset.component.html',
  styleUrls: ['./add-asset.component.css']
})
export class AddAssetComponent {

  Asset = new currentAssets();
  submitted = false;

  constructor(
    private assetService: AssetService,
    private location: Location
  ) { }

  changedate(): void {
	alert("format date");
	this.Asset.buydate = dateFormat(this.Asset.buydate, 'yyyy, mmmm Ds, dddd');
  }
  newAsset(): void {
    this.submitted = false;
    this.Asset = new currentAssets();
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