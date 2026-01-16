class TopHostnamesController {
    /* @ngInject */
    constructor($log, $resource, WireshortDebug) {
        if (WireshortDebug) {
            window.mTopHostnamesController = this; //eslint-disable-line
        }

        this.$log = $log;

        this.resource = $resource('/api/top');

        this.top = null;
    }

    $onInit() {
        this.resource.get()
            .$promise
            .then((response) => {
                this.top = response.top;
            });
    }
}

angular
    .module('wizeshort')
    .component('topHostnames', {
        controller: TopHostnamesController,
        templateUrl: 'top-hostnames/top-hostnames.html'
    });