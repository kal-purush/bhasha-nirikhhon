import {Directive, Output, EventEmitter, HostBinding, HostListener} from '@angular/core';

@Directive({
  selector: '[appDnd]'
})

export class DndDirective {
  @HostBinding('style.stroke') private stroke = 'none';
  @Output() FileDropped = new EventEmitter();

  @HostListener('dragover', ['$event']) onDragOver(event: any): void {
    event.preventDefault();
    event.stopPropagation(); this.stroke = '#2EA3F2';
  }

  @HostListener('dragleave', ['$event']) onDragLeave(event: any): void {
    event.preventDefault();
    event.stopPropagation();
    this.stroke = 'none';
  }

  @HostListener('drop', ['$event']) onDrop(event: any): void {
      event.preventDefault();
      event.stopPropagation();
      const files = event.dataTransfer.files;
      console.log(files);
      if (files.length > 0) {
        this.FileDropped.emit(files);
      }
      this.stroke = 'none';
  }

  constructor() { }

}