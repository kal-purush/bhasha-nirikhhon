import {Component,Input} from "angular2/core";
@Component({
  selector:'my-hero-detail',
  templateUrl: 'app/components/heroDetails/herodetails.html'
})
export class HeroDetailComponent {
  @Input() hero;
}