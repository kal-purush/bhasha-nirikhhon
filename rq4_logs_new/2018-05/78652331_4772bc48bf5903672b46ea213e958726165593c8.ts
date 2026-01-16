import { Component, Input } from '@angular/core';
import { Router } from '@angular/router';

import { ShowInfo } from '../show-info/info.interface';

export class ConfigFewShows {
  title = '';
  path = '';
  items: ShowInfo[] = [];

  constructor(data: ConfigFewShows) {
    this.title = data.title;
    this.path = data.path;
    this.items = data.items;
  }
}

@Component({
  selector: 'app-few-shows-container',
  templateUrl: './few-shows-container.component.html',
  styleUrls: ['./few-shows-container.component.scss']
})
export class FewShowsContainerComponent {
  @Input() config: ConfigFewShows;

  constructor(private router: Router) {

  }

  seeMore(): void {
    if (!this.config.path) {
      return;
    }

    this.router.navigate([this.config.path]);
  }
}