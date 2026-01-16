import {Input, Component, Directive, ElementRef, Renderer2, Output, EventEmitter} from '@angular/core';
import index from '@angular/cli/lib/cli';

@Component({
  selector: 'app-row-component',
  templateUrl: `./row.component.html`,
  styleUrls: [`./row.css`]
})
export class RowComponent {
  @Input() task: any;
  @Input() index: number;

  visibility = false;
  @Output () changedReady = new EventEmitter<boolean>();
  changeReady(change: any) {
    this.changedReady.emit(change);
  }
  @Output () removedItem = new EventEmitter<number>();  //  ?? что  тут  не так ??
  removeItem(remove: any) {
    this.removedItem.emit(remove);
  }
  @Output () saveEditt = new EventEmitter<boolean>();  // todo read about  EventEmitter <boolean>
  saveEdit() {
    this.saveEditt.emit();
  }
  addFromEnter(e) {
    if (e.keyCode === 13) {
      this.validation();
      this.saveEdit();
    } else if (e.keyCode === 27) {
      this.validation();
    }
  }
  validation() {
    if (this.task.text && this.task.text.trim() !== ''  && this.task.deadLine !== '') {
      this.toggle();
    }
  }
  toggle() {
    this.visibility = !this.visibility;
  }
}
