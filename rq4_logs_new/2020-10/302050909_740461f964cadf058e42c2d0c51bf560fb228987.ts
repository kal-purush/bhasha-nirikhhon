import { Component, OnInit } from '@angular/core';
import { Router } from '@angular/router';
import { PopoverController } from '@ionic/angular';
import { YarnTesterViewComponent } from '../../yarn-tester-view/yarn-tester-view.component';

@Component({
  selector: 'utm-inventory-button',
  templateUrl: './inventory-button.component.html',
  styleUrls: ['./inventory-button.component.scss'],
})
export class InventoryButtonComponent implements OnInit {

  constructor(
    private router: Router,
    private popoverController: PopoverController
  ) { }

  ngOnInit() {}

  openInventory() {
    this.router.navigate(['yarn']);
  }

  async openPopover(ev: any) {
    // Open popover
    const popover = await this.popoverController.create({
      component: YarnTesterViewComponent,
      event: ev,
      translucent: true
    });
    return await popover.present();
  }
}