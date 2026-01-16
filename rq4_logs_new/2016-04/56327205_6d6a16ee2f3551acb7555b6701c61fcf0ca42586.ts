import {Component, OnInit} from 'angular2/core'

import {SectionLine} from '../interfaces/section-line.model';

@Component({
    selector: 'cv-section',
    template: '/app/views/cv-section.template.html'
})
export class CvSection implements OnInit{
    lines: SectionLine[];
    
    ngOnInit(){
        this.lines = [
            {date : '1995', content: 'Java'},
            {date : '2005', content: '.NET'}
        ];
    }
    
}