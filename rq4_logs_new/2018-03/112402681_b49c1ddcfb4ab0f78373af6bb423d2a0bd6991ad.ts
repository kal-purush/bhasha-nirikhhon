import { Component } from '@angular/core';
import { DataService } from '../shared/services/data.service';
import { Item } from '../shared/models/item.model';

@Component({
  selector: 'app-sell-item-page',
  templateUrl: './sell-item-page.component.html',
  styleUrls: ['./sell-item-page.component.css']
})
export class SellItemPageComponent {

  item: Item = new Item();

  constructor(private dataService: DataService) { }

  submit() {
    this.item.itemSold = false;
    this.dataService.addNewItem(this.item).subscribe(id => {
      localStorage.setItem('item' + id, this.item.imageSrc);
    });
  }

}