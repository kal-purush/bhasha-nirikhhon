import { Component, OnInit } from '@angular/core';

import { DataService } from '../shared/data.service'

@Component({
    selector: 'education',
    template: require('./education.comp.html'),
    styles: [require('./education.comp.scss'), require('../../styles/main.scss')],
    directives: [],
    providers: [DataService],
})
export class EducationComponent implements OnInit {
    private education: any;

    constructor(private dataService: DataService){
    }

    ngOnInit () {
        this.education = this.dataService.getEducation();
    }
}