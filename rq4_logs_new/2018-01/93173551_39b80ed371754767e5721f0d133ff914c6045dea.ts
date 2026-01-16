import { ExperimentRegistryService } from './../services/experiment-registry';
import { Component, ComponentRef, Injector, Input, OnDestroy, OnChanges, OnInit, SimpleChanges, TemplateRef, ViewChild, ViewContainerRef } from '@angular/core';
import { ExperimentFactoryService } from '../services/experiment-factory';
import { ExperimentGroup } from '../models/experiment';
import { ActivatedRoute } from '@angular/router';

@Component({
  selector: 'group-renderer',
  template: `
  <div class="component" *ngFor="let case of group.cases">
    <component-renderer [caseId]="case.id"></component-renderer>
  </div>
  `,
  styles: [`
    :host {

    }
    .component {
      padding-bottom: 100px;
    }
    `]
})
export class GroupRendererComponent implements OnInit, OnChanges {

  @Input() groupId: string;

  private sub: any;
  private group: ExperimentGroup;

  constructor(private experimentRegistry: ExperimentRegistryService) {

  }

  ngOnInit():void {
    this.group = this.getGroup();
  }

  ngOnChanges(changes: SimpleChanges) {
    for (let propName in changes) {
      let change = changes[propName];
      if (propName === 'groupId') {
        this.group = this.getGroup();
      }
    }
  }

  private getGroup(): ExperimentGroup {
    return this.experimentRegistry.getExperimentGroup(this.groupId);
  }

}