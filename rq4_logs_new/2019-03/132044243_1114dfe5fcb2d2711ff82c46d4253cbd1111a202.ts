import { Component, OnInit } from '@angular/core';
import { Observable} from 'rxjs';
import { Store } from '@ngrx/store';
import { State } from '../app.reducers'
import { Group } from '../models/group';
import { map } from 'rxjs/operators';

@Component({
  selector: 'app-groups',
  templateUrl: './groups.component.html',
  styles: []
})
export class GroupsComponent implements OnInit {
  groups$: Observable<Group[]>;
  total$: Observable<{balance: number, currency: string}[]>;
  selected$: Observable<Group>;
  constructor(private store: Store<State>) {}

  ngOnInit() {
    this.groups$ = this.store.select('groups', 'groups').pipe(map(groups => groups.filter(g => !g.deleted)));
    this.total$ = this.store.select('groups', 'total');
    this.selected$ = this.store.select('groups', 'selected');
  }

  refresh() {
    this.store.dispatch({type:'[groups] query'});
  }

  select(a: Group) {
    this.store.dispatch({type:'[groups] select', payload: a});    
  }

  create() {
    this.store.dispatch({type:'[groups] create'});    
  }

  delete() {
    this.store.dispatch({type:'[groups] delete'});    
  }

  transactions(group: Group) {
    this.store.dispatch({type:'[groups] select', payload: group});    
    this.store.dispatch({type:'[transactions] filter groups', payload: [group]});    
  }

  createTr(ttype: number) {
    this.store.dispatch({type:'[transactions] create', payload: {ttype:ttype}});
  }
}