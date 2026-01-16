/// <reference path="../main.ts" />

module views.index {
    export class IndexViewCtrl {

        constructor($scope, toolbarService:toolbar.ToolbarService) {
            toolbarService.setTitle('Welcome');
        }
    }

    registerController('indexViewCtrl', IndexViewCtrl);
}