import {FlexiFormController} from './flexi-form.controller';

export class FlexiFormComponent implements ng.IComponentOptions {

    public bindings: {[binding: string]: string};
    public controller: ng.IControllerConstructor;
    public template: string;
    public transclude: boolean;

    constructor() {
        this.bindings = {
            configuration: '=',
            customSubmit: '=',
            domainObject: '@'
        };

        this.transclude = true;

        this.controller = FlexiFormController;

        this.template =  require('./flexi-form.tpl');
    }
}