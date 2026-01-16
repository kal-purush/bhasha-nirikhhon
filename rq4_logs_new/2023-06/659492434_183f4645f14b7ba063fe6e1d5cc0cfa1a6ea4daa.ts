import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-counter',
  template: `
  <h3>Counter: {{counter}}</h3>
  <button class="btn btn-primary mx-2" (click)="increaseBy(1)">+1</button>
  <button class="btn btn-primary mx-2" (click)="resetValue()">Reset</button>
  <button class="btn btn-primary" (click)="increaseBy(-1)">-1</button>
  `
})

export class CounterComponent  {
  public title: string = 'Hola Mundo';
  public counter: number = 10;

  increaseBy(value: number):void{
    this.counter += value;
  }

  resetValue():void{
    this.counter = 10;
  }

}