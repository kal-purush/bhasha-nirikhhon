import { Component, OnInit } from '@angular/core';
import { AdminoScreenElement } from '../admino-screen-element';

@Component({
  selector: 'admino-toggle',
  templateUrl: './toggle.component.html',
  styleUrls: ['./toggle.component.scss']
})
export class ToggleComponent extends AdminoScreenElement implements OnInit {

  selectedValue = 0;
  ngOnInit() {
    this.onChange();
  }
  onChange() {
    this.selectedValue = this.element.value;
  }
  toggled(e) {
    this.selectedValue = e.value;
    const foundOption = this.element.options.find((opt) => {
      return opt.value === e.value;
    });
    if (foundOption && foundOption.action) {
      this.handleAction(foundOption.action);
    }
    this.setValue(e.value);
  }
}