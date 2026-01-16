import {Component, Input} from 'angular2/core';

/**
 * TODO: this is janky and only a temporary solution
 */
@Component({
    selector: 'skill-chart',
    template: `
<p>{{getClassification()}}</p>
<ul class="list-inline list-unstyled">
    <li class="skill-detail-reduced-margin" *ngFor="#i of getRating()"></li>
</ul>`,
})
export class SkillChartComponent {
    @Input() rating:number;

    getRating() {
        return new Array(this.rating);
    }

    getClassification() {

        var classification;
        switch(this.rating) {
            case 1:
            case 2:
                classification = "Novice";
                break;
            case 3:
            case 4:
                classification = "Intermediate";
                break;
            case 5:
            case 6:
                classification = "Advanced";
                break;
            case 7:
            case 8:
                classification = "Expert";
                break;
            case 9:
            case 10:
                classification = "Master";
                break;
            default:
                classification ="Other";
                break;

        }
        return classification;
    }
}

