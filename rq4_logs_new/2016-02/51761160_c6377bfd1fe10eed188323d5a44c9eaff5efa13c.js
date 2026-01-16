(function(){
    angular
        .module("app")
        .directive("appFooter", appFooter);

    function appFooter(){
        return {
            restrict: "A",
            templateUrl: "footer/footer.html",
            controller: footerController,
            controllerAs: "footerCtrl"
        }
    }

    footerController.$inject = ["dataService"];

    function footerController (dataService){
        var vm = this;
        vm.footer = dataService.footer;
    }
})();