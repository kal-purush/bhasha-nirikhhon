import { Component } from '@angular/core';
import { AppActions } from './app.actions';
import { AppDispatcher } from './app.dispatcher';
import { AppStore } from './app.store';
import { AppState } from './app.state';

@Component({
  selector: 'my-app',
  template: `
    <hello-world [user]=state.user></hello-world>
  `
})
export class AppComponent {
  private state: AppState;
  constructor(
    private actions: AppActions,
    private dispatcher: AppDispatcher,
    private store: AppStore,
  ) { }

  ngOnInit() {
    this.store.appState.subscribe(s => this.state = s);
  }
}