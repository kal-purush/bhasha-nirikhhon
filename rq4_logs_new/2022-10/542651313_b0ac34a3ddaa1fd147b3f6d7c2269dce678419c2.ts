import { HttpClient } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { Todo } from '../../models/todo';

import {
  HttpClientTestingModule,
  HttpTestingController
} from '@angular/common/http/testing';

describe('TodoService', () => {
  let httpClient: HttpClient;
  let httpTestingController: HttpTestingController;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientTestingModule]
    });

    httpClient = TestBed.inject(HttpClient);
    httpTestingController = TestBed.inject(HttpTestingController);
  });

  afterEach(() => {
    // After every test, assert that there are no more pending requests.
    httpTestingController.verify();
  });

  it('knows how to fetch todos', async () => {
    const testData: Todo[] = [
      { name: 'Todo 1' },
      { name: 'Todo 2' },
      { name: 'Todo 3' }
    ];

    httpClient
      .get<Todo[]>('http://localhost:8080/todos')
      .subscribe(data => expect(data).toEqual(testData));

    const req = httpTestingController.expectOne('http://localhost:8080/todos');

    expect(req.request.method).toEqual('GET');

    req.flush(testData);
  });
});