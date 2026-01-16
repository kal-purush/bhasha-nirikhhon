///<reference path="../../../typings/tsd.d.ts"/>

export = FiletypesController;

class FiletypesController {
    static $inject = [
        '$scope',
        'htManager'
    ];

    constructor(
        $scope,
        public htManager
    ) {
        var unwatch = $scope.$watch('ctrl.htManager.har', (newVal) => {
            if(newVal) {
                this.getFiletypeStats();
                unwatch();
            }
        });
    }

    public stats = null;

    public getFiletypeStats() {
        var array = [];

    }
}