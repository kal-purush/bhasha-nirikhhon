import {Component} from 'angular2/core';
import {ROUTER_DIRECTIVES, RouteConfig, ComponentInstruction} from 'angular2/router';

@Component({
    selector: 'child-comp',
    template: `<h3>Child</h3>`
})
export class ChildComponent {}