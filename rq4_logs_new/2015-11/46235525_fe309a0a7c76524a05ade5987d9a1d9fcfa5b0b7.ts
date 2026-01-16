namespace genericssamples {
  class BaseItem {
    name: string;
  }

  class Repository<T extends BaseItem> {
    private _array: T[];

    constructor(){
      this._array = [];
    }

    add(item: T) {
      this._array.push(item);
      console.log(item.name);
    }

    logAllItems(){
      this._array.forEach((value) => console.log(value.name));
    }
  }

  interface ExtItem extends BaseItem {
    weight: number;
  }

  class ExtendedItem extends BaseItem {
    doSomething(){
      console.log(this.name + " did something");
    }
  }

  let obj = new ExtendedItem();
  obj.name = "name...";

  let cont = new Repository<ExtendedItem>();
  cont.add(obj);
  cont.logAllItems();
}