import { Component, View } from 'angular2/angular2'
import { EventPlayComponent } from './eventPlay-component'

@Component({
  selector: 'input-output'
})
@View({
  templateUrl: 'app/components/componentInputOutputExp/componentInputOutputExp-component.html',
  directives: [EventPlayComponent]
})
export class ComponentInputOutputExpComponent {

  constructor() {
    // Send this data to child
    this.dataForChild = {
      'hereis': 'data from parent to child'
    }
  }

  // Child event triggers this one
  parentReaction (text) {
    console.log('parent: triggered by event from child')
  }
}