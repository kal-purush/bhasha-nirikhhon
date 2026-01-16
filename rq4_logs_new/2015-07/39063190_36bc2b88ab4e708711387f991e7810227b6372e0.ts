/// <reference path="../typed/angular.d.ts" />
/// <reference path="../typed/jquery.d.ts" />
module jDebug {
    export var components: JDebugComponentInterceptor;

    export function initializeDebug() {
        if (!angular || !jasper) {
            console.error('Page must reference AngularJS and JasperJS scripts');
            return;
        }
        var appRootElement = document.querySelector('[ng-app]') ||
            document.querySelector('[j-debug-root]') || document;

        var injector = angular.element(appRootElement).injector();
        if (!injector) {
            console.error('Could not find an injector. Mark application root DOM node with ng-app or j-debug-root attribute');
            return;
        }

        var $compile = <ng.ICompileService>injector.get('$compile');
        var $templateCache = <ng.ITemplateCacheService>injector.get('$templateCache');
        var $http = <ng.IHttpService>injector.get('$http');
        var jasperComponentRegistrar = <jasper.core.HtmlComponentRegistrar>injector.get('jasperComponent');

        components = new JDebugComponentInterceptor($templateCache, $compile, $http);
        jasperComponentRegistrar['setInterceptor'](components);


    }


}


document.addEventListener("DOMContentLoaded", () => {
    jDebug.initializeDebug();
});