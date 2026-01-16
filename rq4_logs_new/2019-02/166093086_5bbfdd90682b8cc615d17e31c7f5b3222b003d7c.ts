import { Component, OnInit, Input } from '@angular/core';
import { NgbModal } from '@ng-bootstrap/ng-bootstrap';
import { ItemComponent } from '../item/item.component';
import { ItemService } from 'src/app/services/item.service';
import { Item } from 'src/app/models/item';

@Component({
  selector: 'card',
  templateUrl: './card.component.html',
  styleUrls: ['./card.component.css']
})
export class CardComponent implements OnInit {
  //Instance variables
  private items: Item[];
  private burials: Item[];
  @Input() isRegularItem: boolean;

  constructor(private modal: NgbModal, private itemService: ItemService) { }

  ngOnInit() {
    //Check to see if we need to get a regular item
    //for the info table or a specific item for the
    //complete list of burials
    if(this.isRegularItem) {
      this.getAllItems();
    } else {
      this.getAllBurials();
    }
  }

  /**
   * Method that will open the modal with the passed
   * in component.
   */
  open(id: number) {
    var modelRef = this.modal.open(ItemComponent);
    
    //Check to see if we are clicking on a regular
    //item or a burial specific item
    if(this.isRegularItem) {
      modelRef.componentInstance.item = this.getItems().find(item => item.id === id);
      modelRef.componentInstance.isRegularItem = this.isRegularItem;
    } else {
      modelRef.componentInstance.item = this.getBurials().find(item => item.id === id);
      modelRef.componentInstance.isRegularItem = this.isRegularItem;
    }
  }

  /**
   * Method that will utilize the item service
   * to retrieve all the items.
   */
  getAllItems() {
    this.itemService.getItems()
      .subscribe(item => this.items = item);
  }

  /**
   * Method that will utilize the item service
   * to retrieve all the items that are burials.
   */
  getAllBurials() {
    //Create the object to send to the API
    let categoryObject = {
      category: 'GRAVE'
    };

    this.itemService.getItemsByCategory(categoryObject)
      .subscribe(burial => this.burials = burial);
  }

  //ACCESSORS
  getItems() {
    return this.items;
  }

  getBurials() {
    return this.burials;
  }
}