  import { Component} from '@angular/core';
/*
@Component({
  selector: 'app-if-exampl',
  template: `<p *ngIf="condition">
                  Привет мир
                 </p>
                 <p *ngIf="!condition">
                   Пока мир
                 </p>
        <button (click) = "tupple()">Toggle</button>`
})*/

@Component({
  selector: 'app-if-exampl',
  template: `<p *ngIf = "condition; else unset">
    Hello Mate!
  </p>
  <ng-template #unset>
    <p>By-by friend</p>
  </ng-template>
  <button (click) = "tupple()">Toggle</button>`
})
export class IfExamplComponent {

  condition = true;

  tupple() {
    this.condition = !this.condition;
  }
}