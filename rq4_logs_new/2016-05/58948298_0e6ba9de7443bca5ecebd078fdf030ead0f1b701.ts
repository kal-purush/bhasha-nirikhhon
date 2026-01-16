import { Component, Input } from '@angular/core';

import { Hero } from '../models/hero';

@Component({
  selector: 'my-hero-detail',
  templateUrl: "app/components/hero-detail.component.html"
})
export class HeroDetailComponent {
  @Input()
  hero: Hero;
}