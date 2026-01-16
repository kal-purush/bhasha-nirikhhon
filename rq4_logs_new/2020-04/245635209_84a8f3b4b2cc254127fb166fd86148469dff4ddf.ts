import { Sorter } from './Sorter';
import { NumbersCollection } from './NumbersCollection';
import { CharactersCollection } from './CharactersCollection';
import { LinkedList } from './LinkedList';

const logSomething = (toBelogged: string | number[]): void =>
  console.log(toBelogged);

logSomething('Ciao!...');
logSomething('Hi There!...');
logSomething('I am Javad :)');

console.log('==========================');

const numbersCollection = new NumbersCollection([23, -1, 5, -9, 0, 48, 2]);
numbersCollection.sort();
logSomething(numbersCollection.data);

const charactersCollection = new CharactersCollection('SuXpo4&');
charactersCollection.sort();
logSomething(charactersCollection.data);

const linkedList = new LinkedList();
linkedList.add(-100);
linkedList.add(3);
linkedList.add(35);
linkedList.add(-2);
linkedList.sort();
linkedList.print();