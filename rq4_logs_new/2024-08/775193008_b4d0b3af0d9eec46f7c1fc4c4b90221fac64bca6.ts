import Dexie, { Table } from 'dexie';

export interface UserData {
  id?: number;
  name: string;
  profilePicture: string;
  dataNcto: string; //yyyy/mm/dd
  genero: string;
}
export interface TodoItem {
  id?: number;
  todoListId: number;
  title: string;
  done?: boolean;
}

export class AppDB extends Dexie {
  usuario!: Table<UserData, number>;

  constructor() {
    super('ngdexieliveQuery');
    this.version(3).stores({
      todoLists: '++id',
      todoItems: '++id, todoListId',
    });
  }

}

export const db = new AppDB();
