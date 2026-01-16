import { Component, OnInit } from '@angular/core';
import { AdminoScreenElement } from '../admino-screen-element';
import { ScreenElementChange } from '../../admino-screen.interfaces';

@Component({
  selector: 'admino-code',
  templateUrl: './code.component.html',
  styleUrls: ['./code.component.scss']
})
export class CodeComponent extends AdminoScreenElement implements OnInit {
  editorOptions = { theme: 'vs-dark', language: 'javascript' };

  ngOnInit() {
    this.onChange(null);
    // var func = function (a) {
    //   return a * a;
    // }
    // const result = func(6);
    // this.createFunc();
  }
  // createFunc() {
  //   var func = new Function("return " + this.element.value)();
  //   // const func = eval(this.element.value);
  //   console.log(func(6))
  // }
  onChange(changes: { [id: string]: ScreenElementChange; }) {
    // if (changes.value) {
    //   this.control.setValue(this.element.value);
    // }
    if (this.element.config) {
      Object.assign(this.editorOptions, this.element.config);
    }
  }
}