import {Directive, TemplateRef} from '@angular/core';

@Directive({selector: '[nglModalHeader]'})
export class NglModalHeaderTemplate {
  constructor(public templateRef: TemplateRef<any>) {}
}

@Directive({selector: '[nglModalTagline]'})
export class NglModalTaglineTemplate {
  constructor(public templateRef: TemplateRef<any>) {}
}

@Directive({ selector: '[nglModalFooter]' })
export class NglModalFooterTemplate {
  constructor(public templateRef: TemplateRef<any>) { }
}