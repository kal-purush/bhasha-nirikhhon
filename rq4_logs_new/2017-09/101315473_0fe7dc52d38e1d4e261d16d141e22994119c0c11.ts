import { Component, EventEmitter, Output, Input, OnInit } from '@angular/core';

@Component({
    selector: 'app-filter',
    templateUrl: './filter.component.html'
})

export class FilterComponent implements OnInit {
    @Output() changed: EventEmitter<String>;
    filter: string;

    constructor() {
        this.changed = new EventEmitter<String>();
    }

    filterChanged(event: any) {
        event.preventDefault();
        // console.log(`Filter Changed: ${this.filter}`);
        this.changed.emit(this.filter);
    }

    ngOnInit() {
    }
}