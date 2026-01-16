export class Todo {
  description: string;
  done: boolean = false;
  
  constructor(description, done) {
    this.description = description;
    this.done = done;
  }
}