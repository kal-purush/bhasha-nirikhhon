import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';
import { Todo, TodoCategory } from '../app/todos/todo';
import { TodoService } from '../app/todos/todo.service';


@Injectable()
export class MockTodoService extends TodoService {
  static testTodos: Todo[] = [
    {
      _id: 'chenfei_id',
      owner: 'Chenfei',
      status: false,
      body: 'He is going to beewehjejh tonight.',
      category: 'software design',
    },
    {
      _id: 'harry_id',
      owner:'Harry',
      status: true,
      body: 'Metal under tension beggin you to touch and go',
      category: 'homework',
    },
    {
      _id: 'kk_id',
      owner: 'KK',
      status: true,
      body: 'Highway to the Danger Zone ride into the Danger Zone',
      category: 'video games',
    },
    {
      _id: 'Peter_id',
      owner: 'Peter',
      status: false,
      body: 'Headin into twilight spreadin out her wings tonight',
      category: 'groceries',
    }
  ];

  constructor() {
    super(null);
  }

  getTodos(filters: { category: TodoCategory; owner?: string; body?: string }): Observable<Todo[]> {
    // Our goal here isn't to test (and thus rewrite) the service, so we'll
    // keep it simple and just return the test users regardless of what
    // filters are passed in.
    //
    // The `of()` function converts a regular object or value into an
    // `Observable` of that object or value.
    return of(MockTodoService.testTodos);
  }

  getTodoById(id: string): Observable<Todo> {
    // If the specified ID is for the first test user,
    // return that user, otherwise return `null` so
    // we can test illegal user requests.
    if (id === MockTodoService.testTodos[0]._id) {
      return of(MockTodoService.testTodos[0]);
    } else {
      return of(null);
    }
  }
}