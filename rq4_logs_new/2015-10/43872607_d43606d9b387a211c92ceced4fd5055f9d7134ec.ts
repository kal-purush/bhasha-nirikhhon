import { Component, View } from 'angular2/angular2'
import { ROUTER_DIRECTIVES, RouteConfig } from 'angular2/router'

import { DumdumComponent } from './dumdum/dumdum-component'
import { BabyComponent } from './baby/baby-component'

@Component({
  selector: 'child-router-exp'
})
@View({
  templateUrl: 'app/components/childRouterExp/childRouterExp-component.html',
  directives: [ROUTER_DIRECTIVES]
})
@RouteConfig([
  {
    path: '/dumdum',
    as: 'Dumdum',
    component: DumdumComponent
  },
  {
    path: '/baby',
    as: 'Baby',
    component: BabyComponent
  }
])
export class ChildRouterExpComponent {}