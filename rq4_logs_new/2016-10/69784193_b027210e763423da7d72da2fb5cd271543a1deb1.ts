import { Component, Input } from '@angular/core';
import { Repository } from '../app.state';
import { AppActions } from '../app.actions';
import { AppDispatcher } from '../app.dispatcher';

@Component({
  selector: 'select-repository',
  template: `
    <h1>select your repos.</h1>
    <input type="button" value="add" (click)="add()" />
    <div *ngFor="let repo of repositories">
      <input [(ngModel)]="repo.owner" />
      <input [(ngModel)]="repo.name" />
    </div>
    <input type="submit" value="save" (click)="onSubmit($event)"/>
  `
})
export class SelectRepositoryComponent {
  @Input() repositories: Repository[];
  constructor(
    private actions: AppActions,
    private dispatcher: AppDispatcher
  ){}
  add() {
    this.repositories.push({ owner: '', name: '' });
  }
  onSubmit() {
    this.dispatcher.emit(this.actions.saveRepositories(this.repositories));
  }
}