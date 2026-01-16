import { Component, Input, OnInit, OnChanges, TemplateRef, ContentChildren, AfterViewInit, AfterContentInit, QueryList, ViewContainerRef, ComponentFactoryResolver, ComponentRef} from '@angular/core';
import { DgTemplateDirective } from './template.directive';
import { Router, ActivatedRoute } from '@angular/router';

@Component({
  selector: 'action', 
  template: 'Of course this template is fake!'
})
export class ActionComponent implements  AfterContentInit {
  @Input() public type: string;

  @Input() public onAction: Function;

  @Input() public confirm: string;

  @ContentChildren(DgTemplateDirective) public templates: QueryList<any>;

  public actionTemplate: TemplateRef<any>;

  constructor(
    private router: Router,
    protected route: ActivatedRoute
  ){}

  protected loadTemplates() {
      this.templates.forEach((dgTemplate: DgTemplateDirective) => {
        // @todo check type is valid
        this[dgTemplate.getType()+"Template"] = dgTemplate.template;
      });
  }

  public ngAfterContentInit():void {
    this.loadTemplates();
  }

  public onClick(item)
  {
    if (this.confirm) {
      if (confirm(this.confirm)) { // @ToDo use sexy modals and enable confirm eval
        this.doAction(item);
      }

      return;
    }

    this.doAction(item);
  }

  private doAction(item)
  {
    if (this.onAction) {
      return this.onAction(item);
    }

    // @Todo make it more customizable
    this.router.navigate([this.route.snapshot.routeConfig.path, item.id, this.type ]);
  }
}