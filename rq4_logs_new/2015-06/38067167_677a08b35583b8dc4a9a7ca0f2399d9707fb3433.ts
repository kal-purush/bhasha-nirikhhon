// GENERICS

class ListOfNumbers {
  _items:number[] = [];
  
  add(item:number) {
    this._items.push(item);
  }
  
  getItems():number[] {
    return this._items;
  }
}

class ListOfStrings {
  _items:string[] = [];
  
  add(item:string) {
    this._items.push(item);
  }
  
  getItems():string[] {
    return this._items;
  }
}

// Two classes above could benefit from generics, since they use same method names,
// but with different types. Think of it as a template.

// method template definition
function add<T>(item:T) {
  // code here
}

// calling the method
add<number>(1234);
add<string>('asdf');


// CREATING A GENERIC CLASS
class List<T> {
  _items:T[] = [];
  
  add(item:T) {
    this._items.push(item);
  }
  
  getItems():T[] {
    return this._items;
  }
}

var accountList = new List<IAccount>();


// GENERIC CONSTRAINTS
class List<T extends IAccount> {
  // this means that whatever type the list is,
  // it has to implement the IAccount interface
}