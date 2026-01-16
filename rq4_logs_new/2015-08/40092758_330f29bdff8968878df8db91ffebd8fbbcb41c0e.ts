/// <reference path="../_all.ts" />
module app {
        export class ToDoItem implements ng.IDirective {
                public link: (scope: ng.IScope, element: ng.IAugmentedJQuery, attrs: ng.IAttributes) => void;
                public template = '<div>Boom Boom Pow</div>';
                public scope = {};

                constructor(/*list of dependencies*/) {
                        // It's important to add `link` to the prototype or you will end up with state issues.
                        // See http://blog.aaronholmes.net/writing-angularjs-directives-as-typescript-classes/#comment-2111298002 for more information.
                        ToDoItem.prototype.link = (scope: ng.IScope, element: ng.IAugmentedJQuery, attrs: ng.IAttributes) => {
                                /*handle all your linking requirements here*/
                        };
                }

                public static factory() {
                        var directive = (/*list of dependencies*/) => {
                                return new ToDoItem(/*list of dependencies*/);
                        };

                        directive['$inject'] = [];

                        return directive();
                }
        }
}