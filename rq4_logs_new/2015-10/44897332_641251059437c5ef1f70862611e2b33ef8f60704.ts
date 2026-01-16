import {Component, View, NgIf} from 'angular2/angular2';

@Component({
    selector: 'modal-layer',
    template: `
    
  `,
    styles: [`
        .overlay {
          top: 0;
          right: 0;
          bottom: 0;
          left: 0;
          position: fixed;
          opacity:0.5 !important;
          z-index: 1;
        }
    `],
    directives: [NgIf]
})
    //TODO
export class ModalLayer {
    
    constructor() {
            
    }
    
}