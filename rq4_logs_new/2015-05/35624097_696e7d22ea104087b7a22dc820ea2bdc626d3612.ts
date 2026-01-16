/// <reference path="../typings/deku/deku.d.ts" />

import Deku = require('deku');
var e = Deku.element;

export var name = "Counter";

export interface P extends Deku.PropLike {
  count: number;
}

export interface S {
  accumulation: number;
}

export function initialState(): S { return { accumulation: 0 }; };
export var defaultProps = { count: 0 };

export function beforeMount(component: Deku.Component<P, S>) {
  console.log("Before Mount: ");
  console.log(component);
}

export function afterMount(component: Deku.Component<P, S>, el: Node, setState: (newState: S) => void) {
  console.log("After Mount: ");
  console.log(component);
  console.log(el);
}

export function shouldUpdate(component: Deku.Component<P, S>, nextProps: P, nextState: S): boolean {
  return (component.props.count !== nextProps.count);
}

export function beforeRender(component: Deku.Component<P, S>): any {
  console.log("Before Render: ");
  console.log(component);
}

export function render(component: Deku.Component<P, S>, setState: (state: S) => void): Deku.VirtualNode<P, {}> {
  var currentCount = component.props.count;
  var currentAcc   = component.state.accumulation;
  return e("div", {}, [currentCount.toString() + " / " + currentAcc.toString()]);
}

export function afterRender(component: Deku.Component<P, S>, el: Node): any {
  console.log("After Render: ");
  console.log(component);
  console.log(el); 
}

export function beforeUpdate(component: Deku.Component<P, S>, nextProps: P, nextState: S): any {
  console.log("Before Update: ");
  console.log(component);
  console.log(nextProps);
  console.log(nextState);
}

export function afterUpdate(component: Deku.Component<P, S>, prevProps: P, prevState: S, setState: (newState: S) => void): void {
  console.log("After Update: ");
  console.log(component);
  console.log(prevProps);
  var newState = { accumulation: component.state.accumulation + component.props.count }; 
  setState(newState);
  console.log(prevState);
  console.log(newState);
}