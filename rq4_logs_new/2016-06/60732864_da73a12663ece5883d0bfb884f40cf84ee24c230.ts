import { Component } from '@angular/core';
import { NgForm } from '@angular/common';

import { HeroForm } from '../shared';

@Component({
  selector: 'ngp-hero-form',
  template: require('./heroForm.component.html'),
  styles: [require('./heroForm.component.scss')],
})
export class HeroFormComponent {
  powers = ['Really smart', 'Super flexible', 'Super hot', 'Weather changer'];
  model = new HeroForm(18, 'Dr. IQ', this.powers[0], 'Chuck Overstreet');
  submitted = false;
  active = true;

  onSubmit() {
    this.submitted = true;
  }

  newHero() {
    this.model = new HeroForm(42, '', '');
    this.active = false;
    setTimeout(() => this.active = true, 0);
  }

  get diagnostics() {
    return JSON.stringify(this.model);
  }
}