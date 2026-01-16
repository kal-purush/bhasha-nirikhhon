import { Component } from '@angular/core';
import { PopoverController, NavParams } from 'ionic-angular';
import { NavProfilePopover } from "../../components/popovers/nav-profile-popover";

/**
 * Generated class for the LogoNavMenuComponent component.
 *
 * See https://angular.io/api/core/Component for more info on Angular
 * Components.
 */
@Component({
  selector: 'logo-nav-menu',
  templateUrl: 'logo-nav-menu.html'
})
export class LogoNavMenuComponent {

  text: string;

  constructor(public popoverCtrl: PopoverController) { }

  showPortfolioMenu(event) {
    let popover = this.popoverCtrl.create(NavProfilePopover);
    popover.present({
      ev: event
    });
  }

}