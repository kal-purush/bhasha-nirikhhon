import {Component, OnInit} from '@angular/core';

@Component({
  selector: 'navbar',
  template: `
        <nav class="navbar navbar-expand-lg navbar-dark bg-dark">
        <a class="navbar-brand" href="#">
            msg.Helps
        </a>

        <button class="navbar-toggler" type="button"
                (click)="toggleNavbar()">
            <span class="navbar-toggler-icon"></span>
        </button>


        <div class="collapse navbar-collapse"
            [ngClass]="{ 'show': navbarOpen }">
            <ul class="navbar-nav mr-auto">

            <li class="nav-item">
                <a class="nav-link" routerLink="/">Home</a>
            </li>

            <li class="nav-item">
                <a class="nav-link" routerLink="/user">Profile</a>
            </li>

            <li class="nav-item">
                <a class="nav-link" routerLink="/help-requests">Requests</a>
            </li>
            </ul>
        </div>
        </nav>
  `,
  styleUrls: ['./navbar-component.scss']
})
export class NavbarComponent {

    navbarOpen = false;

    toggleNavbar() {
        this.navbarOpen = !this.navbarOpen;
    }


}