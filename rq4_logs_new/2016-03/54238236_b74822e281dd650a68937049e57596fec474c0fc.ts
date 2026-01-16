import { Component, OnInit } from 'angular2/core';
import { RouteParams } from 'angular2/router';
import { GuildService } from '../../services/guild/guild';
import { TheGuildComponent } from '../the-guild/the-guild';

@Component({
  selector: 'home',
  moduleId: 'components/home/home',
  template: require('./home.html'),
  styles: [require('./home.scss')],
  providers: [GuildService],
  directives: [
    TheGuildComponent
  ]
})
export class HomeComponent implements OnInit {
  constructor(
    private _GuildService: GuildService,
    private _routeParams: RouteParams) {
  }

  ngOnInit() {
    let sectionKey = this._routeParams.get('sectionKey');
    console.log('sectionkey', sectionKey);
  }
}