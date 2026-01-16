import {Component, OnInit} from '@angular/core';
import {HTTP_PROVIDERS}    from '@angular/http';
import {ROUTER_DIRECTIVES} from '@angular/router';

import {isLoggedin}        from './authentication/is-loggedin';

@Component({
    selector: 'navbar',
    template: `
        <nav [hidden]="hideNavbar">
            <div style="float: left;">
                <!-- <a [routerLink]="['/workbench']">Workbench</a> -->
                <a href="/workbench">Workbench</a>
                <a href="../../cbra/cbrarequests/">Create Case</a>
                <!-- <a href="/reports">Reports</a> -->
                <a [routerLink]="['/tags']">Tags</a>
            </div>
            <div style="float: right;">
                <!-- <input type="text">
                <button>Search</button>
                <button>Reset</button> -->
                User: {{ first_name }} {{ last_name }} <a [routerLink]="['/logout']" (click)="onLogout()">Logout</a>
            </div>
        </nav>
    `,
    directives: [ROUTER_DIRECTIVES],
    providers: [HTTP_PROVIDERS]
})
export class NavbarComponent implements OnInit {
    hideNavbar:Boolean = false;
    first_name: string;
    last_name: string;

    ngOnInit() {
        this.hideNavbar = !isLoggedin();
        this.first_name = sessionStorage.getItem('first_name');
        this.last_name = sessionStorage.getItem('last_name');
    }

}