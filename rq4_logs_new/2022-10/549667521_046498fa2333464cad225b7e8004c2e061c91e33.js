import { action, computed, makeAutoObservable, observable } from 'mobx';
import { nanoid } from 'nanoid';

class TodoStore {
  list = [];
  constructor() {
    makeAutoObservable(this, {
      total: computed,
      list: observable,
      addItem: action,
      checkCompleted: action,
    });
  }

  get total() {
    return this.list.length;
  }

  get unComplete() {
    return this.list.filter((item) => !item.completed).length;
  }

  addItem(value) {
    this.list = [...this.list, { id: nanoid(), value, completed: false }];
  }

  checkToggle(id) {
    const current = this.list.filter((item) => item.id === id)[0];
    current.completed = !current.completed;
  }
}

const todoStore = new TodoStore();

export default todoStore;