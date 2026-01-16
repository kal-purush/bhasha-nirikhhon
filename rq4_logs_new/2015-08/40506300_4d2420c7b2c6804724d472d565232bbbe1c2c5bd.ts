/// <reference path="../typings/tsd.d.ts" />

import {Component, View, bootstrap} from 'angular2/angular2';

@Component({
  selector: 'informacao-a-ser-escondida'
})
@View({
  template: `
    <p>Mussum ipsum cacilds, vidis litro abertis.
    Consetis adipiscings elitis. Pra lá , depois divoltis porris, paradis.
    Paisis, filhis, espiritis santis. Mé faiz elementum girarzis, nisi eros vermeio, in elementis mé pra quem é amistosis quis leo.
    Manduma pindureta quium dia nois paga.
    Sapien in monti palavris qui num significa nadis i pareci latim.
    Interessantiss quisso pudia ce receita de bolis, mais bolis eu num gostis.</p>
  `,
  styles: [
    `
       p {
         max-width: 500px;
         border: 1px solid #ddd;
         padding: 10px;
       }
    `
  ]
})

class InformacaoASerEscondidaCmp {
  constructor() {
    console.log('informacao-a-ser-escondida inicializada')
  }
}

@Component({
  selector: 'toggle'
})
@View({
  template: `
    <div>
       <button type="text" (click)="toggle()">
           click para acionar o toggle
       </button>

       <div [hidden]="visibilidade">
          <ng-content </ng-content>
       </div>

    </div>
  `
})

class ToggleCmp {
  visibilidade: boolean = false; // visível

  constructor() {
    console.log('toggle-cmp inicializado')
  }

  toggle() {
    this.visibilidade = !this.visibilidade;
  }
}

@Component({
  selector: 'toggle-wrapper'
})
@View({
  template: `
    <hr />
    <h2>toggle</h2>
    <toggle>
      <informacao-a-ser-escondida></informacao-a-ser-escondida>
    </toggle>
  `,
  directives: [InformacaoASerEscondidaCmp, ToggleCmp]
})

export class ToggleWrapper {
  constructor() {
    console.log('toggle-wrapper inicializado')
  }
}

bootstrap(InformacaoASerEscondidaCmp)
    .then(() => bootstrap(ToggleCmp))
    .then(() => console.log('toggle cmp criado corretamente'))
    .catch((erro) => console.log(`erro ao criar o toggle ${erro}`))