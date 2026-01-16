import { Directive, HostBinding, Input } from '@angular/core';
import { APP_TAILWIND_STYLES } from './styles';

@Directive({
  selector: '[xcInput]'
})
export class InputDirective {
  private twClasses = '';
  @HostBinding('class')
  @Input()
  set xcInput(klasses: inputTypes) {
    this.twClasses = (!klasses ? 'inputPrimary' : klasses)
      .split(' ')
      .map((k) => APP_TAILWIND_STYLES[k] ?? k)
      .join(' ');
  }
  get xcInput(): String {
    return this.twClasses;
  }

}

type inputTypes = 'inputPrimary';