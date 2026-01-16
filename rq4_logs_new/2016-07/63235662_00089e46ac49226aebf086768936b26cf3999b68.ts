import { Component } from '@angular/core';
import { NavController } from 'ionic-angular';

/*
  Generated class for the ReloginPage page.

  See http://ionicframework.com/docs/v2/components/#navigation for more info on
  Ionic pages and navigation.
*/
@Component({
  templateUrl: 'build/pages/relogin/relogin.html',
})
export class ReloginPage {
    relogin = {
        password: <string> null
    }

    constructor(private nav: NavController) {}

    changeUser() {}

    verifyRelogin(reloginForm) {}
}