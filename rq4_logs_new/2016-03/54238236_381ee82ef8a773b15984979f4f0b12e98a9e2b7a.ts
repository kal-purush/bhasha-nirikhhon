import { Component, OnInit } from 'angular2/core';
import { RouteParams } from 'angular2/router';

import { Hero } from '../../interfaces/hero';
import { GuildService } from '../../services/guild/guild';

@Component({
  selector: 'raiding',
  moduleId: 'components/raiding/raiding',
  template: require('./raiding.html'),
  styles: [require('./raiding.sass')]
})
export class RaidingComponent implements OnInit {
  constructor(
    private _routeParams: RouteParams) {
  }

  ngOnInit() {
    let sectionKey = this._routeParams.get('sectionKey');
  }
}