class UserListDirective {
    restrict = "E";
    templateUrl = "core/user/components/list/list.html";
    controller = UserListController;
    controllerAs = "users";
    bindToController = true;
}

class UserListController {
    public list: any;

    constructor() {
        this.list = [
            { name: "Sebastian" },
            { name: "Francisco" }
        ];
    }
}

function directive() {
    return new UserListDirective();
}

angular.module("app.user").directive("userList", directive);