import { Component, OnInit } from '@angular/core';

import { DataService } from '../shared/data.service'

@Component({
    selector: 'techs',
    template: require('./tech.comp.html'),
    styles: [require('./tech.comp.scss'), require('../../styles/main.scss')],
    directives: [],
    providers: [DataService],
})
export class TechsComponent implements OnInit {
    private techs: any;

    constructor(private dataService: DataService){
    }

    ngOnInit () {
        this.techs = this.dataService.getTech();
    }
}