

class App {
    static main() {
        angular
            .module("RouteDemo", ['ngRoute'])
            .controller("AppCtrl", AppCtrl)
            .config(this.ConfigRoutes)      
    }

    static ConfigRoutes($routeProvider: ng.route.IRouteProvider) {
        $routeProvider
            .when("/home", {
                action: "home.default"
            })
            .when("/range/:line/:column", {
                action: "range.line"
            })
            .when("/search/:term", {
                action: "search.term"
            })
            .when("/search/:term/select/:result", {
                action: "search.term.result"
            })
    }
}

class AppCtrl {
    // alias services with 'any' type
    private _route: any;
    private _routeParams: any;

    constructor(
        private $scope: ng.IScope,
        private $route: ng.route.IRoute,
        private $routeParams: ng.route.IRouteParamsService 
        ) {
        this._route = $route;
        this._routeParams = $routeParams;

        $scope.$on("$routeChangeSuccess",
            ($currentRoute, $previousRoute) => {
                this.updateState();
            });
    }

    updateState() {
        var current = this._route.current;
        console.log("$route.current", current && current.$$route );
        console.log("$routeParams", this.$routeParams);
    }
}


//$(() => App.main());
App.main();