/// <reference path="../typings/tsd.d.ts />

import {Component, View, Directive} from 'angular2/angular2';

@Directive({
    selector: '[diretiva-qualquer]',
    host: {
        '(input)': 'inputHandlerDiretiva()'
    }
})
class EventoDeInputComBubbleDiretiva {
    inputHandlerDiretiva() {
        console.log('diretiva chamada');
    }
}


@Component({
    selector: 'evento-de-input-com-bubble'
})
@View({
    template: `
        <hr />
        <h3>Input - Com bubble</h3>
        <input type="text" diretiva-qualquer (input)="inputHandlerCmp()"/>
        <p [inner-text]="logDoInput"></p>
    `,
    directives: [EventoDeInputComBubbleDiretiva]
})

export class EventoDeInputComBubbleCmp {
    logDoInput: string = '';

    inputHandlerCmp() {
        this.logDoInput = `Input aconteceu em: ${new Date()}}`;
    }
}