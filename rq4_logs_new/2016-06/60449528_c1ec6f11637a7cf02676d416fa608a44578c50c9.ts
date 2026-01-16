import {Component} from '@angular/core';
import {ROUTER_DIRECTIVES, Routes} from '@angular/router';
import {LoginComponent} from './lib/login.component';
import {AdminPanelComponent} from './lib/admin-panel.component';
import {CategoryListComponent} from './lib/category/category-list.component';
import {CategoryComponent} from './lib/category/category.component';
import {InventoryListComponent} from './lib/inventory/inventory-list.component';
import {InventoryComponent} from './lib/inventory/inventory.component';

@Component({
    selector: 'warehouse-app',
    template: `
        <ul>
            <li><a [routerLink]="['/login']">Login</a></li>
            <li><a [routerLink]="['/categories']">Categories</a></li>
            <li><a [routerLink]="['/inventories']">Inventories</a></li>
        </ul>
        <router-outlet></router-outlet>
    `,
    directives: [ROUTER_DIRECTIVES]
})
@Routes([
    {path: '/login', component: LoginComponent},
    {path: '/admin', component: AdminPanelComponent},
    {path: '/categories', component: CategoryListComponent},
    {path: '/category/create', component: CategoryComponent},
    {path: '/category/:id', component: CategoryComponent},
    {path: '/inventories', component: InventoryListComponent},
    {path: '/inventory/create', component: InventoryComponent}
])

export class AppComponent {
}