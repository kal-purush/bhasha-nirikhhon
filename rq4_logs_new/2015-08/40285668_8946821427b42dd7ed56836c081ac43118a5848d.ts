module stanby.directives.forms.validations {
    export function initFormValidations() {
        //NOTE(JH): Adding to the same stanbyDirectives module.
        angular.module('stanbyDirectives')

        /**
         * This directive delays the update to the model by 550ms for the default, and also
         * updates on blur immediately. Because of the delayed update, it also binds to the
         * 'enter' key to update the model immediately so validation on the form can work
         *
         * Important:
         * - the form must use ng-submit. eg <form ng-submit=""> for enter key and validation to work
         * - place form validation on the ng-submit. eg <form ng-submit="form.$valid && ctrl.save(form)">
         *
         * Any pending changes take place immediately when the form is submitted via the submit event.
         * ngClick events will occur before the model is updated so use ng-submit instead.
         * As the update model is debounced, validators aren't executed until the debounce is done, or the
         * form is submitted. So ng-disabled="form.$invalid" doesn't work for disabling the submit button.
         * Instead we use ng-submit="form.$valid && ctrl.save(form)" which will prevent invalid values
         * from being submitted.
         */
        .directive('stDelayedUpdate', [function() {
            return {
                restrict: 'A',
                require: 'ngModel',
                link: function(scope, element, attrs, ctrl) {
                    var delay = 550;
                    if (attrs.stDelayedUpdate != "") {
                        delay = parseInt(attrs.stDelayedUpdate);
                    }
                    if (!ctrl.$options) {
                        ctrl.$options = {
                            updateOn: 'blur',
                            updateOnDefault: true,
                            debounce: {
                                'blur': 0,
                                'default': delay
                            }
                        }
                    }
                    // updates the model immediately when enter is pressed
                    element.bind('keydown', function (e) {
                        if (e.keyCode === 13) {
                            ctrl.$commitViewValue();
                        }
                    });
                }
            }
        }])
    }
}