import {Component, View, NgIf} from 'angular2/angular2';

@Component({
    selector: 'modal',
    template: `
    <div class="modal" *ng-if="isOpen">
      <button class="right" (click)="close()">X</button>
      <ng-content></ng-content>
    </div>
    <div class="overlay" *ng-if="isOpen" (click)="close()"></div>
  `,
    styles: [`
        .modal {
          background-color: #A9D5EF; 
          padding: 10px;
          width: 50%;
          height: 300px;
          text-align: center;
          left: 25%;
          position: relative;
          z-index: 1000;
        }
        .right {
          float: right;
        }
        .overlay {
          top: 0;
          right: 0;
          bottom: 0;
          left: 0;
          position: fixed;
          background-color: black;
          z-index: 1;
        }
    `],
    directives: [NgIf]
})
export class Modal {
    
    isOpen: boolean = false;

    open() {
        this.isOpen = true;
    }

    close() {
        this.isOpen = false;
    }
}