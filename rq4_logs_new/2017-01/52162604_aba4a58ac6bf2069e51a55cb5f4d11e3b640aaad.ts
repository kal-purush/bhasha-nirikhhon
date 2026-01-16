import {Component, Input} from '@angular/core';
import {Volunteer} from './experience';

@Component({
    selector: 'volunteer',
    templateUrl: 'app/components/experience/volunteer.component.html',
})
export class VolunteerComponent {
    @Input() volunteers:Volunteer[];
}