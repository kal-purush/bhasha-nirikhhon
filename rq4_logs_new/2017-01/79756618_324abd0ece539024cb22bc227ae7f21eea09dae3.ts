import { Component, OnInit } from '@angular/core';

@Component({
    selector: 'about',
    templateUrl: 'about.component.html'
})

export class AboutComponent implements OnInit {

    message: string = 'Hello from about!!';

    constructor() { }

    ngOnInit() {
    }
}