import {Component, ElementRef, NgModel, FORM_DIRECTIVES, CORE_DIRECTIVES} from 'angular2/angular2';
import {Http, Response} from 'angular2/http';

@Component({
    selector: 'plan-list',
    directives: [FORM_DIRECTIVES, CORE_DIRECTIVES],
    template: `
    <ul>
        <li *ng-for="#plan of plans"> {{plan.name}}</li>
    </ul>
    `
})
export class PlanList {
    private plans;

    constructor(private http: Http) {
        this.plans = [];
        this.http.get('data/plans.json')
            .subscribe(resp => this.setPlans(resp.json()));
    }

    private setPlans(plans) {
        console.log(plans);
        this.plans = plans;
    }
}