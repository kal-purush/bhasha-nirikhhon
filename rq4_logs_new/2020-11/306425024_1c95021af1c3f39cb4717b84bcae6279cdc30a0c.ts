/**
 * This component demonstrates how we can disable sidebar for some subroutes.
 *
 * For example this component disables sidebar for routes "/micro-frontends-angular-route/no-sidebar/*".
 */
import { Component, OnInit } from '@angular/core';
import {
  disableSidebarForRoute,
  enableSidebarForRoute,
} from '@topcoder/micro-frontends-navbar-app';

const COMPONENT_ROUTE = "/micro-frontends-angular-route/no-sidebar/*";

@Component({
  selector: 'tc-ex-no-sidebar-demo',
  templateUrl: './no-sidebar-demo.component.html',
})
export class NoSidebarDemoComponent implements OnInit {
  route = COMPONENT_ROUTE
  enable = () => enableSidebarForRoute(COMPONENT_ROUTE)
  disable = () => disableSidebarForRoute(COMPONENT_ROUTE)

  ngOnInit(): void {
    disableSidebarForRoute(COMPONENT_ROUTE)
  }
}