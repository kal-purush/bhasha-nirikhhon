interface IModalCtrlScope extends ng.IScope {
    menu: IMenu;
    customMenuColors: CustomMenuColors;
    close();
}

interface IModalMenuService {
    launch(truckId:string, colors:CustomMenuColors): void;
}

angular.module('TruckMuncherApp').factory('modalProfileImageService', ['$modal', ($modal) => new ModalMenuService($modal)]);

class ModalMenuService implements IModalMenuService {
    $modal:ng.ui.bootstrap.IModalService;

    constructor($modal:ng.ui.bootstrap.IModalService) {
        this.$modal = $modal;
    }

    launch(imageUploadUrl:string):void {
        var modalCtrl = ['$scope', 'truckId', 'customMenuColors', '$modalInstance', 'MenuService',
            function ($scope:IModalCtrlScope, truckId:string, customMenuColors:CustomMenuColors, $modalInstance:ng.ui.bootstrap.IModalServiceInstance, MenuService:IMenuService) {
                $scope.menu = null;
                $scope.customMenuColors = customMenuColors;

                MenuService.getMenu(truckId).then(function (response) {
                    $scope.menu = response.menu;
                });

                $scope.close = function () {
                    $modalInstance.close({});
                };
            }];

        this.$modal.open({
            templateUrl: "/partials/map/customer-menu.jade",
            controller: modalCtrl,
            resolve: {
                imageUploadUrl: function () {
                    return imageUploadUrl;
                }
            }
        });
    }
}