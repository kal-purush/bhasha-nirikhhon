import { Component, NgFor, FORM_DIRECTIVES } from 'angular2/angular2';
import { Hero } from './hero';

@Component({
	selector: 'hero-form',
	templateUrl: 'app/hero-form.component.html',
	directives: [NgFor, FORM_DIRECTIVES]
})
export class HeroFormComponent {
  powers = ['Really Smart', 'Super Flexible',
	  'Super Hot', 'Weather Changer'];
		
	model = new Hero(1, 'Dr IQ', this.powers[0], 'Chuck Overstreet');		
  submitted = false;
	
	onSubmit() {
	  this.submitted = true;
	}
	
  // NOTE: property for debug purpose
	get diagnostic() { return JSON.stringify(this.model); }
}