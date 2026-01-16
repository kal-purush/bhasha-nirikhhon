import { Component, Input, OnInit } from '@angular/core';
import { faTrophy, faMedal, faExternalLinkAlt, faPencilAlt } from '@fortawesome/free-solid-svg-icons';

@Component({
    selector: 'app-timeline',
    templateUrl: './timeline.component.html',
    styleUrls: ['./timeline.component.scss']
})
export class TimelineComponent implements OnInit {
    faTrophy = faTrophy;
    faMedal = faMedal;
    faLink = faExternalLinkAlt;
    faPencil = faPencilAlt;
    @Input() language: string;

    constructor() { }

    ngOnInit(): void {
    }

}