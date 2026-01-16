import { Component, Query, QueryList, CORE_DIRECTIVES } from 'angular2/angular2'

// https://angular.io/docs/ts/latest/api/core/Query-var.html

@Component({
  selector: 'pane',
  inputs: ['title']
})
class Pane {
  title:string
}

@Component({
  selector: 'tabs',
  directives: [CORE_DIRECTIVES],
  template: `
    <ul>
      <li *ng-for="#pane of panes">{{pane.title}}</li>
    </ul>
    <content></content>
  `
})
class Tabs {
  panes: QueryList<Pane>
  constructor(@Query(Pane) panes:QueryList<Pane>) {
    this.panes = panes
  }
}

// The main class

@Component({
  selector: 'query-exp',
  directives: [Tabs],
  template: `
    <h3>query exp</h3>
    <tabs></tabs>
  `
})
export class QueryExp {
  constructor () {
    console.log('testing')
  }
}