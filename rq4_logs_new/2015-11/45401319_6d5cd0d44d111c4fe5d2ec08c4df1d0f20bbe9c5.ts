import {Component,NgFor} from 'angular2/angular2';
import {NavBarComponent} from './navbar.component';
import {SliderComponent} from './slider.component';

@Component({
  selector: 'mixer',
  template: `
    
    <div class="panel panel-default">
    
      <slider *ng-for="#og of optiongroups" [option-group]="og"></slider>
    
    </div>
    `,
  directives: [NgFor, NavBarComponent, SliderComponent]
})
export class MixerComponent  {
  public title = 'hello world';
  public optiongroups:Array<IOptionGroup> = [];
  constructor() {
    this.optiongroups[0] = {name:'SMS', value:50};
    this.optiongroups[1] = {name:'Surf',value:100};
    this.optiongroups[2] = {name:'Min',value:300};
  }
}