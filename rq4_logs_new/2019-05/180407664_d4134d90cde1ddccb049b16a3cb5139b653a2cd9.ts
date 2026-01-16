import { Component, ViewChild } from '@angular/core';
import { NavController, NavParams, Nav } from 'ionic-angular';
import { SearchbarServiceProvider } from '../../providers/searchbar-service/searchbar-service';

import { PollInterfacePage } from '../../pages/poll-interface/poll-interface';
import { LoginPage } from '../../pages/login/login';
import { LogoutPage } from '../../pages/logout/logout';
import { RegisterPage } from '../../pages/register/register';
import { DashboardPage } from '../../pages/dashboard/dashboard';
import { PollResultsPage } from '../../pages/poll-results/poll-results';

/**
 * Generated class for the SearchbarComponent component.
 *
 * See https://angular.io/api/core/Component for more info on Angular
 * Components.
 */
@Component({
  selector: 'searchbar',
  templateUrl: 'searchbar.html'
})
export class SearchbarComponent {
  @ViewChild(Nav) nav: Nav;
  

  rootPage: any = LoginPage;

  pages: Array<{title: string, component: any}>;

  text: string;

  constructor(public search$: SearchbarServiceProvider, public navCtrl: NavController, public navParams: NavParams) {
    console.log('Hello SearchbarComponent Component');
    this.text = 'Hello World';
  
  // used for an example of ngFor and navigation
  this.pages = [
    { title: 'Login', component: LoginPage },
    { title: 'Logout', component: LogoutPage },
    { title: 'Register', component: RegisterPage },
    { title: 'Dashboard', component: DashboardPage },
    { title: 'Results', component: PollResultsPage},
    { title: 'PollInterface', component: PollInterfacePage}
  ];
  
  }

  openPage(page) {
    // Reset the content nav to have just this page
    // we wouldn't want the back button to show in this scenario
    this.nav.setRoot(page.component);
  }
  
  goToAccountInfo() {
    this.nav.setRoot(DashboardPage);
  }
  
  goToRewardsHistory() {
    this.nav.setRoot(DashboardPage);
  }
  goToLogout() {
    this.nav.setRoot(DashboardPage);
  }

}