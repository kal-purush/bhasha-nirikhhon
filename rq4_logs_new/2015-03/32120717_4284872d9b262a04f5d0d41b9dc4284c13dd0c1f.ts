/// <reference path="../../bower_components/forms-js/dist/forms-js.d.ts" />

module adaptor.directives {

  export function ViewDirective($compile: ng.ICompileService, builder):ng.IDirective {
    var formsjsForm:formsjs.Form;

    return {
      restrict: 'AE',
      scope: true,
      link: function(scope:any,
                     element:ng.IAugmentedJQuery,
                     attrs:any) {

        scope.$watch(attrs.view || attrs.fjsView, function(view) {
          if (view !== undefined) {

            var frag = builder(view);
            element[0].appendChild(frag);
            $compile(element.children())(scope);
          }
        });

      }
    };
  }
}