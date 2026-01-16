import { Component, OnInit, Input } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { Location } from '@angular/common';
import { InventoryService } from '../../../services/inventory.service';
import { InventoryListItem } from '../../../models/inventory-list-item';
import { StateList } from '../../../models/state';

@Component({
  selector: 'inventory-edit',
  templateUrl: './inventory-edit.component.html',
  styleUrls: ['./inventory-edit.component.scss']
})
export class InventoryEditComponent implements OnInit {
  stateList: StateList = {
    state: 'save',
    routerLink: ''
  };

  constructor(
    private route: ActivatedRoute,
    private _inventoryService: InventoryService,
    private location: Location,
    private router: Router
  ) { }

  ngOnInit() {
  }

  save() {
    console.log('SAVE!');
    // this.goBack();
  }

  delete() {
    console.log('DELETE!');
  }

  cancel() {
    this.goBack();
  }

  goBack(): void {
    this.location.back();
  }
}