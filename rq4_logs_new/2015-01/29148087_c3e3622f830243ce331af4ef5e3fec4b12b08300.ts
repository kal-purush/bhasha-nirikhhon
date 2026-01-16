module Utils {
    export class Sovereignties {

        private _data;
        constructor() {
            d3.json('data/sovereignty.topo.json', (error, data) => {
                if (!error) {
                    var sovereignty = topojson.feature(data, data.objects.sovereignty);
                    this._data = sovereignty;
                    var geom = data.objects.sovereignty.geometries;
                    this.drawmap('sovereignties');
                }
            })
        }

        private drawmap(containerName: string) {
            var width = 800;
            var height = 600;
            var svg = d3.select('#' + containerName)
                .append('svg')
                .attr({
                    'width': width,
                    'height': height
                });

            var mercatorProjection = d3.geo.mercator()
                .scale(120)
                .translate([width/2, height/2]);

            var path = d3.geo.path()
                .projection(mercatorProjection);

            svg.selectAll('.sovereignty')
                .data(this._data.features)
                .enter()
                .append('path')
                .attr('d', path)
            .style({
                fill: (d) => {
                    // Asia,Africa,Europe,South America,Antarctica,North America,Oceania,Seven seas (open ocean)
                    var color = '#222';
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



        }
    }
}