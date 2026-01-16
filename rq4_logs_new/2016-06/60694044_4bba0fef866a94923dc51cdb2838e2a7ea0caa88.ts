import { Component, Input, OnInit } from 'angular2/core';
import { RouteParams } from 'angular2/router';

import { Collaborator } from './collaborator';
import { CollaboratorService } from './collaborator.service';

@Component({
  selector: 'my-collaborator-detail',
  templateUrl: 'app/collaborator-detail.component.html',
  styleUrls: ['app/collaborator-detail.component.css']
})
export class CollaboratorDetailComponent implements OnInit {
  @Input() collaborator: Collaborator;

  constructor(
    private _collaboratorService: CollaboratorService,
    private _routeParams: RouteParams) {
  }

  ngOnInit() {
    let id = +this._routeParams.get('id');
    this._collaboratorService.getCollaborator(id)
      .then(collaborator => this.collaborator = collaborator);
  }

  goBack() {
    window.history.back();
  }
}