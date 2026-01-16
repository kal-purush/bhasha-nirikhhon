import { Component, View } from 'angular2/angular2'
import { CanActivate, CanDeactivate, OnActivate, OnDeactivate } from 'angular2/router'

@Component({
  selector: 'router-hooks'
})
@View({
  template: '<h3>Router Hooks Experiment</h3>'
})
@CanActivate(() => {
  return confirm('can activate?')
})
export class RouterHooksExpComponent implements CanDeactivate, OnActivate, OnDeactivate {
  canDeactivate (next, prev) {
    return confirm('can deactivate?')
  }

  onActivate (next, prev) {
    console.log('onActivate triggered')
    console.log(next, prev)
  }

  onDeactivate (next, prev) {
    console.log('onDeactivate triggered')
    console.log(next, prev)
  }
}