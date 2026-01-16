/// <reference path="../typings/angularjs/angular.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />
/// <reference path="./histogram.ts" />

interface IDataScope extends ng.IScope {
    histConfig: HistConfig;
    data: Array<number>;
    mv: DataCtrl;
    N: number;
}
class DataCtrl {
    constructor(private $scope: IDataScope) {
        // Functions to test out $watch on `data`
        $scope.histConfig = {
            xDomain: [0, 1],
            xScale: 'linear',
            yScale: 'linear',
        }
        $scope.mv = this;
        $scope.N = 100;
        this.hatDist($scope.N);
    }
    hatDist(n: number) {
        var i;
        this.$scope.data = new Array(n)
        for (i=0; i<n; i++) {
            var tmp = Math.random() + Math.random();
            this.$scope.data[i] = tmp/2;
        }
    }
    moreDist(n: number) {
        var i;
        this.$scope.data = new Array(n)
        for (i=0; i<n; i++) {
            var j, tmp=0;
            for (j=0; j<10; j++)
                tmp += Math.random();
            this.$scope.data[i] = tmp/10;
        }
    }
}

angular.module('histogramApp', ['histModule'])
    .controller('histData', DataCtrl)
;