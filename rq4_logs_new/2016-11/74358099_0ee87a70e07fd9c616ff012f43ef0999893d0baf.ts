import { Injectable } from '@angular/core';
import { Http, Headers } from '@angular/http';
import  "rxjs/Rx";

@Injectable()
export class TodoService {

  constructor(public http:Http) { }

  getTodos(){
  	return this.http.get('http://localhost:3000/api/v1/todos');
  }

  saveTodo(todo){
  	let headers = new Headers();
  	console.log(headers);
  	headers.append('Content-Type', 'application/json');
  	return this.http.post('http://localhost:3000/api/v1/todo', JSON.stringify(todo),
  		{headers: headers})
  		.map(res => res.json());
  }

  updateTodo(todo){
  	let headers = new Headers({ 'Content-Type': 'application/json' });
  	let url = 'http://localhost:3000/api/v1/todo/'+todo._id;
  	console.log(url);
  	return this.http.put(url, JSON.stringify(todo),
  		{headers: headers})
  		.map(res =>res.json());
  }

  deleteTodoServer(id){
  	let url = 'http://localhost:3000/api/v1/todo/'+id;
  	return this.http.delete(url)
	  	.map(res => res.json);
  }

}