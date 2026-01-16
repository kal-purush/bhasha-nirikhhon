import { Component, OnInit, Input, ChangeDetectionStrategy } from '@angular/core';

import { Inventory } from '../../models/inventory.interface';

@Component({
    selector: 'app-item',
    styleUrls: ['item.component.scss'],
    changeDetection: ChangeDetectionStrategy.OnPush,
    template: `
        <div class="inventory_item">
            <p>{{ item.json.itemName }}</p>
            <img src="{{ imageUrl() }}">
            <p>{{ item.json.itemDescription}}
        </div>
    `
})

export class ItemComponent implements OnInit {
    @Input()
    item;

    constructor() { }

    ngOnInit() { }

    imageUrl() {
      return `https://www.bungie.net${this.item.json.icon}`
    }
}