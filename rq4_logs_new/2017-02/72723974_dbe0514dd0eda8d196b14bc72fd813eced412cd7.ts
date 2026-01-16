import {TagsValidator} from "./tags.validator";
export class TagsValidatorDirective implements ng.IDirective {
    require = "ngModel";

    constructor() {
    }

    link: ng.IDirectiveLinkFn = (scope, el: angular.IAugmentedJQuery, attrs: any, ctrl: any): void => {
        ctrl.$validators.tagging = function (modelValue, viewValue) {
            if (modelValue) {
                for (let model of modelValue) {
                    if (!TagsValidator.isValid(model[attrs.displayedProperty])) return false;
                }
            }

            return true;
        };
    }
}