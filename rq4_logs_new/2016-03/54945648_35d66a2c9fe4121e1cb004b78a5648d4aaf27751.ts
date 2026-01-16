import {Component} from 'angular2/core';

@Component({
    selector: 'proposition-form',
    templateUrl: 'app/proposition-form.component.html'
})

export class PropositionFormComponent{
    public name = undefined;
    public time = undefined;
    public place = undefined;
    public proposing = false;

    propose() {
        this.proposing = true;

        Materialize.toast(this.name + ' ' + this.time + ' ' + this.place, 4000);
        this._resetForm();
    }
    _resetForm() {
        this.name = undefined;
        this.time = undefined;
        this.place = undefined;
        //this.proposing = false;
    }
}