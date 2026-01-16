module Utils {
    export interface ISovereigntiesScope extends ng.IScope {
        vm: ISovereignties;
    }

    export interface ISovereignties {
        isLoading: boolean;
        countries: Array<any>;

        translateX :number;
        translateY:number;
        scale :number;

    }
    export class Sovereignties implements ISovereignties {

        private _data;
        isLoading: boolean;
        countries: Array<any>;

        translateX = 350;
        translateY = 350;
        scale = 120;

        mapGroup: D3.Selection;

        constructor(private $scope: ISovereigntiesScope) {
            $scope.vm = this;
            this.isLoading = true;
            this.countries = [];
            var self = this;
            d3.json('data/sovereignty.topo.json', (error, data) => {
                if (!error) {
                    var sovereignty = topojson.feature(data, data.objects.sovereignty);
                    self._data = sovereignty;
                    //var test = _.where(sovereignty.features, (s) => {
                    //    return s.properties.continent[0].toLowerCase() === 'e';
                    //});
                    //self._data.features = test;
                    var geom = data.objects.sovereignty.geometries;
                    var properties = _.pluck(self._data.features, 'properties');
                    properties.forEach((p) => {
                        self.countries.push(p.name);
                    })
                    self.drawmap('sovereignties');
                    self.isLoading = false;
                    self.$scope.$apply();

                }
            })
        }

        private drawmap(containerName: string) {
            var width = $('#'+containerName).width();
            var height = 600;
            var svg = d3.select('#' + containerName)
                .append('svg')
                .attr({
                    'width': width,
                    'height': height
                });
            this.mapGroup = svg.append('g');

            var mercatorProjection = d3.geo.mercator()
                .scale(1)
                .translate([0, 0]);

            var path = d3.geo.path()
                .projection(mercatorProjection);

            var enter = this.mapGroup.selectAll('.sovereignty')
                .data(this._data.features)
                .enter();
            var g = enter.append('g').classed('country', true);
            var self = this;
            g.append('path')
                .attr('d', path)
                .style({
                    fill: (d) => {
                        // Asia,Africa,Europe,South America,Antarctica,North America,Oceania,Seven seas (open ocean)
                        var color = '#ccc';
                        if (d.properties.continent === 'Europe') {
                            color = 'blue';
                        } else if (d.properties.continent === 'Asia') {
                            color = 'yellow';
                        } else if (d.properties.continent === 'Oceania') {
                            color = 'Green';
                        }
                        return color;
                    }
                });
            //g.append('text')
            //    .attr("transform", function (d) { return "translate(" + path.centroid(d) + ")"; })
            //    .style({
            //        'text-anchor':'middle'
            //    })
            //    .text(d=> { return d.properties.name;});
            //var continents = _.pluck(this._data.features, 'properties');
            //continents = _.uniq(continents, 'continent');
            //continents = _.pluck(continents, 'continent');
            //console.log(continents);
            //svg.selectAll('.country-label')
            //    .data(this._data.features)
            //    .enter()
            //    .append('text')
            //    .attr({
            //        'dy': '.35em',
            //        'transform': (d) => { return 'translate(' + mercatorProjection(d.geometry.coordinates) + ')'; }

            //    })
            //    .text((d) => { return d.properties.name; })

            this.update();

        }

        update() {
            this.mapGroup
            .transition()
                .attr({
                    'transform': 'translate(' + this.translateX + ',' + this.translateY + ')'+
                    'scale('+this.scale+')'
            });
        }
    }

    var app = angular.module('app');
    app.controller('Sovereignties', ['$scope', ($scope) => new Sovereignties($scope)]);
}