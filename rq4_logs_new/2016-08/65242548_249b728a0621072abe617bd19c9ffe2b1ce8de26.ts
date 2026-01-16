import {Component, EventEmitter, Output, Input} from 'angular/core'


// Child Component
@Component({
    selector: 'tbody',
    template: `
        <td>{{ test }}</td>
        <td>
          <select (change)="selectedActionChange($event)">
            <option *ngFor="#opt of optionList">{{ opt }}</option>
          </select>
        </td>
        <td class="below">
          <input type="checkbox" [ngModel]="checkboxValue" (ngModelChange)="checkboxChanged($event)" />
        </td>
    `
})
export class ChildComponent {
  @Input() test: string;
  @Input() checkboxValue: boolean;
  @Input() selectedAction: string;
  @Output() action: EventEmitter<any> = new EventEmitter();
  @Output() checked: EventEmitter<any> = new EventEmitter();
  optionList: Array<string> = ["Add", "Replace", "Skip"];
  
  selectedActionChange(event) {
    this.action.emit({
      value: event.srcElement.value,
      test: this.test
    });
  }
  
  checkboxChanged(event: any) {
    this.checked.emit({
        value: event,
        test: this.test
    });
  }
  
}