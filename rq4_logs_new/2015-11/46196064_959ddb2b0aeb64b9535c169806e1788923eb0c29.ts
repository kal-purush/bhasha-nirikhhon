import {Component, FORM_DIRECTIVES} from 'angular2/angular2';

@Component({
    selector: 'filter-by',
    directives: [ FORM_DIRECTIVES],
    template: `
    <form>
        Search ({{needle}}):
        <input
            value="12"
            [(ng-model)]="needle"
            >
    </form>
    `
})

export class FilterBy () {
    public needle = "Initid";
    constructor() {
        this.needle = 'In contructor';
    }
}