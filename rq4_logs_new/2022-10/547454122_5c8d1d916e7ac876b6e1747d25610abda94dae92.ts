/*  Module 6: DGenerics in TypeScript
    Lab Start */

/*  DataStore is a utility function that can store up to 10 string values in an array. 
    Rewrite the DataStore class so the array can store items of any type.
    TODO: Add and apply a type variable. */
class DataStore<T> {
  private _data: Array<T> = new Array(10);

  AddOrUpdate(index: number, item: T) {
    if (index >= 0 && index < 10) {
      this._data[index] = item;
    } else {
      console.log("Index is greater than 10");
    }
  }

  GetData(index: number) {
    if (index >= 0 && index < 10) {
      return this._data[index];
    } else {
      return;
    }
  }
}

let cities = new DataStore();

cities.AddOrUpdate(0, "Mumbai");
cities.AddOrUpdate(1, "Chicago");
cities.AddOrUpdate(11, "London"); // item not added

console.log(cities.GetData(1)); // returns 'Chicago'
console.log(cities.GetData(12)); // returns 'undefined'

// TODO Test items as numbers.
let numbers = new DataStore<number>();

numbers.AddOrUpdate(0, 1);
numbers.AddOrUpdate(1, 2);
numbers.AddOrUpdate(2, 3);
numbers.AddOrUpdate(3, 4);

console.log(numbers.GetData(1));
console.log(numbers.GetData(2));

// TODO Test items as objects.
interface Pessoa {
  nome: string;
  idade: number;
}

let pessoas = new DataStore<Pessoa>();
let caio: Pessoa = { nome: "Caio", idade: 23 };
let icaro: Pessoa = { nome: "Ícaro", idade: 22 };

pessoas.AddOrUpdate(0, caio);
pessoas.AddOrUpdate(1, icaro);

console.log(pessoas.GetData(0));
console.log(pessoas.GetData(1));