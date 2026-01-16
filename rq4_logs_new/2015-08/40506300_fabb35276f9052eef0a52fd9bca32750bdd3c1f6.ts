/// <reference path="../typings/tsd.d.ts" />

import {Component, View, EventEmitter, Directive, ElementRef, Inject} from 'angular2/angular2';

@Directive({
  selector: '[gabarito]',
  host: {
      '(input)': 'respostaHandler($event)'
  },
  events: ['respostaCerta', 'respostaErrada'],
  bindings: [EventEmitter]
})
class RespostaDirective {
  respostaCerta: EventEmitter;
  respostaErrada: EventEmitter;
  elemento: ElementRef;

  constructor(@Inject(ElementRef) el: ElementRef) {
    this.respostaCerta = new EventEmitter();
    this.respostaErrada = new EventEmitter();
    this.elemento = el;
  }

  respostaHandler() {
    if (this.elemento.nativeElement.value === "81") {
      this.respostaCerta.next({});
    }
    else {
      this.respostaErrada.next({});
    }
  }
}

@Component({
  selector: 'evento-customizado'
})
@View({
  template: `
    <hr/>
    <h3>evento-customizado</h3>
    <label>9x9 é...</label>
    <input type="text" gabarito (resposta-certa)="aeeeee()" (resposta-errada)="erou()"/>
    <p [inner-text]="resultado" ></p>
  `,
  directives: [RespostaDirective]
})

export class EventoCustomizadoCmp {
  resultado: string = '';

  aeeeee() {
    this.resultado = 'Acertô, mizeravi!'
  }

  erou() {
    this.resultado = 'Ô loko, bicho! Errou!';
  }

}