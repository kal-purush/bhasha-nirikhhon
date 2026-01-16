import { Component, Input, OnInit } from '@angular/core';
import { AlignmentType, Highlight } from 'src/app/core/types/highlight.type';

@Component({
    selector: 'app-section-highlight',
    templateUrl: './section-highlight.component.html',
    styleUrls: ['./section-highlight.component.scss']
})
export class SectionHighlightComponent implements OnInit {
    @Input() highlight: Highlight;
    @Input() alignment: AlignmentType;

    constructor() {}

    ngOnInit(): void {}
}