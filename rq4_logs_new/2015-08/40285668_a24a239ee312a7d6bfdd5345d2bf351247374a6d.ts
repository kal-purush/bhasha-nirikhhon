module stanby.directives.users.inputs {
    export function initFormInputs() {
        //NOTE(JH): Adding to the same stanbyDirectives module.
        angular.module('stanbyDirectives')
        /*
         A directive that builds the roles checkbox for the user's page. Binds to a controller function
         to perform updates
         */

        .directive('stFormsRolesCheckBox', [function() {
            return {
                restrict: 'E',
                scope: {
                    userRoles: "=userRoles",
                    roleName: "=roleName",
                    allRoles: "=allRoles",
                    updateFunction: "&"
                },
                template: '<input type="checkbox" value="{{::roleName}}"><label ng-click="addRole()">{{::realname}}</label>',
                link: function(scope, element, attrs) {
                    // add role function within the scope of the directive
                    scope.addRole = function() {
                        /*
                        this function calls directives updateFunction() which is a function that was
                        passed in by when declaring this directive (see edit.html.ejs)
                        NOTE(JH): The following map of local variable names must match what's defined
                        when using the directive:
                            update-function="controllerFunctionToCall(role, selected)"
                        */
                        var $checkboxEl = element.find('input');
                        var checked = $checkboxEl.prop('checked') ? false : true;
                        $checkboxEl.prop('checked', checked);
                        scope.checkbox.checked = checked;
                        scope.updateFunction({role: scope.roleName, selected: scope.checkbox.checked});
                    }

                    var inRole = scope.userRoles.indexOf(scope.roleName) > -1;
                    scope.checkbox = element.children();
                    scope.label = scope.checkbox.next()[0];
                    // set the default checked state
                    scope.checkbox.checked = inRole;
                    scope.checkbox.prop('checked', inRole);
                    // set the name of the label
                    scope.realname = scope.allRoles[scope.roleName].name;
                }
            }
        }])
    }
}