import { Directive, ViewRef, ViewContainerRef, TemplateRef } from 'angular2/angular2'

@Directive({
  selector: '[assign-local]',

  inputs: [
    'local: assignLocalTo'
  ],
})

export class AssignLocal {
  view: ViewRef;

  constructor(private viewContainer: ViewContainerRef, private templateRef: TemplateRef) {
  }

  set local(expression: string) {
     if (!this.viewContainer.length) {
       this.view = this.viewContainer.createEmbeddedView(this.templateRef);
     } else {
       this.view = this.viewContainer.get(0);
     }

    this.view.setLocal("$implicit", expression);
  }
}