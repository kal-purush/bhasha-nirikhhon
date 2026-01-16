import {Component, EventEmitter, Input, Output} from '@angular/core';
import {FormGroup} from '@angular/forms';

@Component({
  selector: 'app-comment-text-field',
  template: `
    <div class="form-group" [formGroup]="form" (ngSubmit)="onSubmit()">
      <div class="submit-wrapper text-right">
        <input (click)="onSubmit()" type="submit" class="btn btn-sm btn-outline-primary" [disabled]="!form.valid" value="Comenteaza"/>
      </div>
      <textarea [formControlName]="controlName" class="form-control" rows="3"
                placeholder="Adauga un comentariu..."></textarea>
    </div>
  `,
  styleUrls: ['./comment-text-field.component.scss']
})
export class CommentTextFieldComponent {
  @Input() form: FormGroup;
  @Input() controlName: string;
  @Output() postMessage = new EventEmitter<string>();

  onSubmit() {
    if (this.form.valid) {
      this.postMessage.emit(this.form.value[this.controlName]);
    }
  }
}