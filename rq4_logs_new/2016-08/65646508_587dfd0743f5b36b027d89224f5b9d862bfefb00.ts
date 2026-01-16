import { Component, OnInit } from '@angular/core';

import { DataService } from '../shared/data.service'

@Component({
    selector: 'aboutme',
    template: require('./aboutme.comp.html'),
    styles: [require('./aboutme.comp.scss'), require('../../styles/main.scss')],
    directives: [],
    providers: [DataService],
})
export class AboutmeComponent implements OnInit {
    private aboutme: any;

    constructor(private dataService: DataService){
    }

    ngOnInit () {
        this.aboutme = this.dataService.getAboutme();
    }
}