import { Component, Input, ViewChild, ElementRef } from 'angular2/angular2'
import { ChildOfSomeInput } from './childOfSomeInput'

@Component({
  selector: 'some-input',
  directives: [ChildOfSomeInput],
  template: `
    <p>{{mytext}}</p>
    <p>{{wadda}}</p>
    <child-of-some-input></child-of-some-input>
  `,
  inputs: ['wadda']
})
export class SomeInput {

  @Input() mytext: string

  @ViewChild(ChildOfSomeInput) childOfSomeInput: ChildOfSomeInput

  // ElementRef is referencing this component's element!
  constructor (el:ElementRef) {
    console.log('some input init')
    console.log(el)
    // child of some input is not available yet!
  }

  // here you can reach the children of this component
  afterViewInit () {
    this.childOfSomeInput.lasseSays()
    console.log(this.childOfSomeInput)
  }
}