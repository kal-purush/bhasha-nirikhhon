import { Component } from '@angular/core';
import { NavigationService } from '../../core/services/navigation.service';

@Component({
  selector: 'navbar',
  template: `
 			 <nav *ngIf="nav.visible" class="navbar navbar-expand-lg navbar-light bg-light">
			    <a class="navbar-brand" href="#">
			      <img src="../assets/png/maprules.png"
			        width="30"
			        height="30"
			        class="d-inline block align-top"
			        alt="" />
			    </a>
			    <app-link navLocation="home"></app-link>
			    <app-link navLocation="new" ></app-link>
			    <app-link navLocation="explore" ></app-link>
			  </nav>
 			
	`
})

export class NavigationBarComponent {

  constructor( public nav: NavigationService ) {}
}