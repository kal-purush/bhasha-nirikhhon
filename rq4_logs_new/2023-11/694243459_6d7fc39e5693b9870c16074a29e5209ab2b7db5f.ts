import { Directive, HostBinding, Input } from '@angular/core';
import { APP_TAILWIND_STYLES } from './styles';

@Directive({
  selector: '[xcTextArea]'
})
export class TextAreaDirective {
  private twClasses = '';
  @HostBinding('class')
  @Input()
  set xcTextArea(klasses: textAreaTypes) {
    this.twClasses = (!klasses ? 'textAreaPrimary' : klasses)
      .split(' ')
      .map((k) => APP_TAILWIND_STYLES[k] ?? k)
      .join(' ');
  }
  get xcTextArea(): String {
    return this.twClasses;
  }
}

type textAreaTypes = 'textAreaPrimary';