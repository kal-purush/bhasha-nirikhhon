import { DOMTemplate } from "./base-component";

interface ListItem {
  isDone: boolean;
  text: string;
}

class TodoCommon extends DOMTemplate {
  constructor(id: string) {
    super(id);
  }
}

class TodoList {
  //single singleton
  private static instance: TodoList;
  private _list: ListItem[] = [];
  get list() {
    return this._list;
  }
  private _todoShow: TodoCommon;
  get todoShow() {
    return this._todoShow;
  }
  private _todoAdd: TodoCommon;
  get todoAdd() {
    return this._todoAdd;
  }
  private constructor() {
    this._todoShow = new TodoCommon("todoListShow");
    this._todoAdd = new TodoCommon("todoListAdd");
    this.triggerTodoType(true);
  }
  static getInstance() {
    if (!TodoList.instance) {
      TodoList.instance = new TodoList();
    }
    return TodoList.instance;
  }

  triggerTodoType(isShow: boolean) {
    if (isShow) {
      this._todoShow.element.className = "to-do-list-show push-active";
      this._todoAdd.element.className = "to-do-list-add pull-leave";
    } else {
      this._todoShow.element.className = "to-do-list-show push-leave";
      this._todoAdd.element.className = "to-do-list-add pull-active";
    }
  }

  addItem() {
    const input = this._todoAdd.element.querySelector(
      "#todoItem"
    ) as HTMLTextAreaElement;
    if (this._list.length > 5) {
      alert("别太贪心，完成以后再添加吧！");
      return;
    } else {
      this._list.push({
        isDone: false,
        text: input.value,
      });
    }

    input.value = "";
  }
  //   removeItem(index: number) {}
  renderList() {
    const input = this._todoShow.element.querySelector("ul")!;
    let dom = "";
    this._list.forEach((item) => {
      !item.isDone ? (dom += `<li>${item.text}</li>`) : "";
    });
    input.innerHTML = dom;
  }
}
const todoList = TodoList.getInstance();
export default todoList;