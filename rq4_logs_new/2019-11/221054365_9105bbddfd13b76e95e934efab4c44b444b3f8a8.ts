import { Directive, TemplateRef } from '@angular/core';

@Directive({
  selector: '[appMenuTemplate]'
})
export class MenuTemplateDirective {

  constructor(public template: TemplateRef<any>) { }

}