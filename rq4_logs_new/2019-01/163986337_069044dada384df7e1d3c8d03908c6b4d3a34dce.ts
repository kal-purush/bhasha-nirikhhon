import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl } from '@angular/forms';
import { Store } from '@ngrx/store';
import { HeaderState } from '../../../header/store/header.reducers';
import { selectTabs } from '../../../header/components/menu/store/menu.selectors';
import { GetTabs } from '../../../header/components/menu/store/menu.actions';

@Component({
  selector: 'addNewPost-component',
  templateUrl: './addNewPost.component.html',
  styleUrls: ['./addNewPost.component.scss']
})
export class AddNewPostComponent implements OnInit {

  addNewPostForm = new FormGroup({
    tab: new FormControl(this.store.select(selectTabs)),
    title: new FormControl(''),
    tags: new FormControl(''),
    content: new FormControl('')
  });

  constructor(
    private store: Store<HeaderState>
  ) { }

  ngOnInit() {
    this.store.dispatch(new GetTabs());
  }
  
  addNewPost() {
    console.log(1);
  }

}