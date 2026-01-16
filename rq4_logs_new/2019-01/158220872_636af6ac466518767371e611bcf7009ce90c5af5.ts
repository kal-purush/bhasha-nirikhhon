import { Directive, Input, OnChanges, Output, EventEmitter, OnInit } from '@angular/core';
import { fromEvent } from 'rxjs';
import { debounceTime,distinctUntilChanged } from 'rxjs/operators';
@Directive({
  selector: '[appSearch]'
})
export class SearchDirective implements OnInit {
  @Input() searchValue: string;
  @Output()sendSearchValue= new EventEmitter();

  constructor() { }
  ngOnInit(){
    this.search();
  }
  search(){
    const input = document.getElementById('search');
    const example = fromEvent(input, 'keyup');
    const debouncedInput = example.pipe(debounceTime(1000));
    const subscribe = debouncedInput.subscribe(val => {
      this.sendSearchValue.emit(this.searchValue);
      // console.log(val)
    });
  }
}