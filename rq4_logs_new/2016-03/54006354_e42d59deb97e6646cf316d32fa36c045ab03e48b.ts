"use strict";
import {Component, Output, EventEmitter} from "angular2/core";

@Component({
    selector: "autocomplete-list",
    template: `<div class="dropdown-menu  search-results">
                    <a *ngFor="#item of list" class="dropdown-item" (click)="onClick(item)">{{item.text}}</a>
               </div>`,
    styles: [".search-results { position: relative; right: 0; display: block; padding: 0; overflow: hidden; font-size: .9rem;}"]
})
export class AutocompleteList  {
    @Output() public selected = new EventEmitter();

    public list;

    public onClick(item: {text: string, data: any}) {
        this.selected.emit(item);
    }
}