import {Injectable} from 'angular2/core';
import {Http, Headers} from 'angular2/http';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class TodosService {
  list: Observable<Array<Object>>;
  local: Observable<Array<Object>>;
  current = '';
  constructor(private http: Http) {
    this.reload();
  }

  reload() {
    this.list = this.http.get('https://zs-todo-api.herokuapp.com/todos')
      .map(data => data.json());
  }

  addTodo() {
    var headers = new Headers();
    headers.append('Content-Type', 'application/json');
    var payload = JSON.stringify({name: this.current});
    this.http.post('https://zs-todo-api.herokuapp.com/todos',
      payload,
      {headers}
      ).subscribe(
        data => this.reload(),
        err => console.log(err),
        () => { this.current = ''; }
      );
  }

}