
import {Item} from "./item";
import {AngularIndexedDB} from "angular2-indexeddb/angular2-indexeddb";

export class BaseComponent {
  protected items: Item[] = [];
  protected new_item: any = '';
  protected table_name: any;
  protected db: AngularIndexedDB = null;
  protected db_name: any;

  protected initItem(): void {
    this.db = new AngularIndexedDB(this.db_name, 1);

    this.db.openDatabase(1, (evt) => {

      console.log(this.table_name);

      evt.currentTarget.result.createObjectStore(
        this.table_name, { keyPath: 'id', autoIncrement: true });


    }).then(result => {

      this.db.getAll(this.table_name).then((items_of_db) => {

        for (const item of items_of_db) {
          this.items.push( new Item(item));
        }

      }, (error) => {

        console.log(error);

      });
    }).catch(error => {

      console.log(error);

    });
  }



  protected addItem (): void {
    if (this.new_item) {

      const item = new Item();
      item.text = this.new_item;

      this.db.add(this.table_name, item).then((evt) => {

        item.id = evt.key;

        this.items.push(item);
        this.new_item = '';
      }, (error) => {
        console.log(error);
      });
    }
  }

  protected deleteItem(id: number): void {
    this.db.delete(this.table_name, id).then(() => {
      this.items  = this.items.filter( item => (item.id !== id));
    }, (error) => {
      console.log(error);
    });
  }

  protected changeItem(id: number): void {
    this.db.getByKey(this.table_name, id).then((item_of_db) => {
      const item = new Item(item_of_db);
      item.changeStatus();

      this.db.update(this.table_name, item).then(() => {

      }, (error) => {
        console.log(error);
      });
    }, (error) => {
      console.log(error);
    });
  }
}