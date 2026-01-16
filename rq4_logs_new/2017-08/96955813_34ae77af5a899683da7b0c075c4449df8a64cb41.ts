import { Component, ChangeDetectionStrategy, OnInit } from '@angular/core';
import { ActivatedRoute } from '@angular/router';

import { Inventory } from '../../models/inventory.interface';
@Component({
    selector: 'app-inventory',
    changeDetection: ChangeDetectionStrategy.OnPush,
    templateUrl: 'inventory.component.html'
})

export class InventoryComponent implements OnInit {
    inventory: Inventory;

    constructor( private route: ActivatedRoute ) { }

    ngOnInit() {
        this.route.data.subscribe( data => {
            this.inventory = data.inventory;
            console.log(this.inventory);
        });
    }
}