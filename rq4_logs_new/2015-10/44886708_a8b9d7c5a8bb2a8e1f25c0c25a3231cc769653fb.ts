import {Component, Input} from 'angular2/angular2';


@Component({
  selector: 'tab',
  template: `
    <div class="tab-pane" [hidden]="!active" style="padding: 5px">
      <ng-content></ng-content>
    </div>`,
  directives: [],
})
export class Tab {

  @Input() tabTitle: string;
  @Input() active: boolean;

}