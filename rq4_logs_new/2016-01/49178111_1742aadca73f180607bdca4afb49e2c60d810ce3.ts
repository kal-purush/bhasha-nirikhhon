import {Component} from 'angular2/core';
import {ContentPaneComponent} from './content-pane.component';
import {NavigationItem} from './navigation-item';
import {SidebarPaneComponent} from './sidebar-pane.component';

@Component({
    selector: 'portfolio',
    template: `
    <h1>{{title}}</h1>
    <sidebar-pane (update)="onNavigationUpdate($event)"></sidebar-pane>
    <content-pane [content]="selectedNavigationItem"></content-pane>
    `,
    directives: [ContentPaneComponent, SidebarPaneComponent],
})


export class AppComponent {
    public title = "I am Michael Dunton!";
    public selectedNavigationItem : NavigationItem;
    onNavigationUpdate(navigationItem) {
        console.log("Setting NavigationItem", navigationItem);
        this.selectedNavigationItem = navigationItem;
    }
}