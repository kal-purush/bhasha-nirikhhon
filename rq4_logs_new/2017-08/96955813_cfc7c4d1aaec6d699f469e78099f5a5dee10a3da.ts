import { Component, OnInit, ChangeDetectionStrategy, Output, EventEmitter } from '@angular/core';

@Component({
    selector: 'app-char-nav',
    styleUrls: ['char-nav.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
    <div class="app-nav">
        <div class="wrapper">
            <a routerLink="inventory" routerlinkActive="active">Inventory</a>
            <a routerLink="activity" routerlinkActive="active">Activity Stats</a>
            <a routerLink="historical" routerlinkActive="active">Historical Stats</a>
            <a (click)="logoutUser()">Logout</a>
        </div>
    </div>
    `
})

export class CharNavComponent implements OnInit {
    id;

    @Output()
    logout = new EventEmitter<any>();

    constructor() {
    }

    ngOnInit() {
    }

    logoutUser() {
        this.logout.emit(true);
    }
}