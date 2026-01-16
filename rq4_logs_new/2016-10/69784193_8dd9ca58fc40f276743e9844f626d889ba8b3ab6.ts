import { Component, Input } from '@angular/core';
import { Label } from '../app.state';

@Component({
  selector: 'issue-label',
  template: `<span [ngStyle]="{ 'background-color': '#' + label.color }">{{label.name}}</span>`,
styles: ['span { color: #2a332a; font-weight: 600; padding: 3px 4px; font-size: 12px; }']
})
export class LabelComponent {
  @Input() label: Label;
}