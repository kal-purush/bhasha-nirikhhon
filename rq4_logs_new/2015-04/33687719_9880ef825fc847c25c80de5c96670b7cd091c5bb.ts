/// <reference path="../typings/angularjs/angular.d.ts" />
/// <reference path="../typings/d3/d3.d.ts" />

module ngHist {

    interface IDataScope extends ng.IScope {
        histConfig: HistConfig;
        data: Array<[number, number]>;
    }
    export interface HistConfig {
        xDomain: Range;
        yDomain?: Range;
        xScale: string;
        yScale: string;
    }
    class DataCtrl {
        constructor(private $scope: IDataScope) {
            // Functions to test out $watch on `data`
            $scope.histConfig = {
                xDomain: [-6, 6],
                xScale: 'linear',
                yScale: 'linear',
            }
        }
    }

    type Range =[number, number];

    export interface IHistScope extends ng.IScope {
        options: any;
        data: any;
        width: number;
        height: number;
        padding: number;
        xDomain: [number, number];
        yDomain: [number, number];
        svg: D3.Selection;
        plot;

        render(): any;
        drawHist();
        drawAxes();
    }
    class HistCtrl {
        hdata: D3.Layout.Bin[];
        xScale: D3.Scale.QuantitativeScale;
        yScale: D3.Scale.QuantitativeScale;
        xDomain: Range;
        yDomain: Range;
        constructor(private $scope: IHistScope) {
            // Things that need to be drawn
            $scope.drawAxes = function () {
                var xAxis = d3.svg.axis()
                    .scale(this.xScale)
                    .ticks(5)
                    .orient("bottom");
                var yAxis = d3.svg.axis()
                    .scale(this.yScale)
                    .ticks(5)
                    .orient("left");

                // Draw the axes
                $scope.svg.append("g")
                    .attr("class", "axis")
                    .attr("transform", "translate(0," + ($scope.height - $scope.padding) + ")")
                    .call(xAxis);
                $scope.svg.append("g")
                    .attr("class", "axis")
                    .attr("transform", "translate(" + $scope.padding + ",0)")
                    .call(yAxis);
            };
            $scope.drawHist = () => {
                var chartBody = $scope.svg.append("g")
                    .attr("clip-path", "url(#plotArea)")

                // This next bit creates an svg path generator
                var pathGen = d3.svg.line()
                    .x(function (d) { return this.xScale(d[0]); })
                    .y(function (d) { return this.yScale(d[1]); })
                    .interpolate("linear");

                var bar = $scope.svg.selectAll(".bar")
                    .data(this.hdata).enter().append("g")
                    .attr("class", "bar")
                    .attr("transform",(d) => {
                        return "translate(" + this.xScale(d.x) + "," +
                            this.yScale(d.y) + ")"
                    });

                bar.append("rect").attr("x", 1)
                    .attr("width",(d) => { return this.xScale(d.dx); })
                    .attr("height",(d) => { return this.yScale(d.y); })
            };
            $scope.render = () => {
                $scope.svg.selectAll('*').remove();
                var p = $scope.padding;
                var w = $scope.width;
                var h = $scope.height;

                // Set up the scales
                this.xScale = d3.scale.linear()
                    .domain(this.xDomain).range([p, w - p]);
                this.yScale = d3.scale.linear()
                    .domain([0, d3.max(this.hdata,(d) => { return d.y; })])
                    .range([h - p, p]);

                $scope.drawAxes();
                $scope.drawHist();
            };

            /* The following sets up watches for data, and config
             */
            $scope.$watch('options',  () => {
                this.setOptions($scope.options);
                $scope.render();
            }, true);
            $scope.$watch('data', () => {
                this.hdata = d3.layout.histogram().bins(this.xScale)($scope.data);
                $scope.render();
            });

            /* We create an apply function (so it can be removed),
             * and force an $apply on resize events, triggering 
             * the `$watch`s in the `link` function.
             */
            var apply = function () { $scope.$apply(); };
            window.addEventListener('resize', apply);
        }

        private setOptions(opts) {
            if (opts) {
                this.xDomain = opts.xDomain || [-1, 1];
                this.yDomain = opts.yDomain || [-1, 1];
            }
            else {
                this.xDomain = [-1, 1];
                this.yDomain = [-1, 1];
            }
        }

    }
    function histDirective(): ng.IDirective {
        /* Sets the options for the plot, such as axis locations, range, etc...
         */

        return {
            restrict: 'EA',
            transclude: true,
            scope: {
                options: '=',
                data: '='
            },
            controller: HistCtrl,
            link: function (scope: IHistScope, elm, attrs) {

                // TODO read plot options
                scope.padding = 30;
                scope.svg = d3.select(elm[0])
                    .append("svg")
                    .attr("height", '100%')
                    .attr("width", '100%');

                scope.width = elm[0].offsetWidth - scope.padding;
                scope.height = elm[0].offsetHeight - scope.padding;

                scope.$watch(function () {
                    return elm[0].offsetWidth;
                }, function () {
                        scope.width = elm[0].offsetWidth - scope.padding;
                        scope.height = elm[0].offsetHeight - scope.padding;
                        scope.render();
                    });
                scope.$watch(function () {
                    return elm[0].offsetHeight;
                }, function () {
                        scope.width = elm[0].offsetWidth - scope.padding;
                        scope.height = elm[0].offsetHeight - scope.padding;
                        scope.render();
                    });

            },
            template: '<div ng-transclude></div>',
        };
    }

    angular.module('plotting', [])
    // This service is pulled from 
        .controller('histData', DataCtrl)
        .directive('histogram', histDirective)
    ;
}