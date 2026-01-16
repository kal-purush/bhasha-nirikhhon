import { TestBed } from '@angular/core/testing';
import { Todo } from '../models/todo';

import { TodoService } from './todo.service';

describe('TodoService', () => {
  let service: TodoService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TodoService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });

  it('knows how to return list todos', () => {
    const todos: Todo[] = service.getTodos();
    expect(todos).toHaveSize(3);
  });
});