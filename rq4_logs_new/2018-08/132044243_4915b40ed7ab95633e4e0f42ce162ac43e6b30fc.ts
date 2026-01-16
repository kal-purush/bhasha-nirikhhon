import { Component, OnInit } from '@angular/core';
import { Observable} from 'rxjs';
import { Store } from '@ngrx/store';
import { State } from '../app.reducers'
import { Category } from '../models/category';

@Component({
  selector: 'app-categories',
  templateUrl: '/categories.component.html',
  styles: []
})
export class CategoriesComponent implements OnInit {
  constructor(private store: Store<State>) {}

  ngOnInit() {
  }

  refresh() {}
}