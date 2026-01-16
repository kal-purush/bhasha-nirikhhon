import userServiceURL = require('../../../../app/modules/core/services/user');
export interface IController {
    test: string,
    someData: string;
    someAction: () => void;
    doSearch: () => void;
    doSearch2: () => void;
}

export class Controller {
    constructor(public $scope: IController, private $route, private $http: ng.IHttpService) {
        this.$scope.test = ":)";
        this.$scope.someData = "this is a test";
        this.$scope.someAction = () => {
            console.log($route.current.locals.Data([], function (response) {
                console.log(response);
            }));
        };
        this.$scope.doSearch = () => {
            debugger;
            alert('Thanks for Calling mE , :)');
        };
        this.$scope.doSearch2 = () => {
            var service = new userServiceURL.UserService(this.$http);
            service.getUserByName('jveldboom').then((response)=>{
                console.log(response);
            });
        };
    }
}