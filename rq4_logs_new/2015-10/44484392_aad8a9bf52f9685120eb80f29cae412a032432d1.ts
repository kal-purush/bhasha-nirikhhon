/// <reference path="../../typings/tsd.d.ts" />

import {
  Component,
  Inject,
  OnInit
} from 'angular2/angular2';

import {WizardShortcutToggler} from 'client/wizard_shortcut/wizard_shortcut_cmp.js';

@Component({
  selector: 'desktop-menu-cmp',
  templateUrl: 'client/desktop_menu/desktop_menu.html',
  styleUrls: ['client/desktop_menu/desktop_menu.css']
})
export class DesktopMenuCmp implements OnInit {
  constructor(@Inject(WizardShortcutToggler) public wizardShortcutToggler: WizardShortcutToggler) {

  }

  onInit() {
      console.log('desktop-menu init');
  }

  showWizardHandler() {
    this.wizardShortcutToggler.show();
  }
}