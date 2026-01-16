/// <reference path="../../ts-definitions/DefinitelyTyped/angularjs/angular.d.ts"/>
/// <reference path="../../ts-definitions/DefinitelyTyped/d3/d3.d.ts"/>

interface WordCloudText {
  key: string;
  value: number;
}

interface IWordCloudScope extends ng.IScope {
  texts: WordCloudText[];
  callback: (data: any) => void;
}

angular.module('egrid')
  .directive('wordCloud', () => {
    return {
      restrict: 'E',
      scope: {
        texts: '=',
        callback: '='
      },
      link: (scope: IWordCloudScope, element: any) => {
        var texts = scope.texts;
        var callback = scope.callback;

        var width = 500;
        var height = 500;
        var fill = d3.scale.category20b();
        var fontSize = d3.scale.linear()
          .domain(d3.extent(texts, text => text.value))
          .range([10, 100]);
        var svg = d3.select(element[0])
          .append('svg')
          .style('width', width + 'px')
          .style('height', height + 'px');
        var contents = svg.append('g')
          .attr('transform', 'translate(' + [width / 2, height / 2] + ')');
        var cloud = (<any>d3).layout.cloud()
          .words(texts)
          .fontSize(d => fontSize(+d.value))
          .text(d => d.key)
          .size([width, height])
          .on('end', function(data, bounds) {
            var scale = bounds ? Math.min(
              width / Math.abs(bounds[1].x - width / 2),
              width / Math.abs(bounds[0].x - width / 2),
              height / Math.abs(bounds[1].y - height / 2),
              height / Math.abs(bounds[0].y - height / 2)) / 2 : 1;
            var text = contents.selectAll('text')
              .data(data, d => d.text.toLowerCase());
            text.enter()
              .append('text')
              .attr({
                'text-anchor': 'middle',
                'transform': (d) => 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')',
              })
              .style('font-size', d => d.size + 'px')
              .on('click', (d) => {
                if (callback) {
                  callback(d.text);
                }
              });
            text
              .style('font-family', (d) => d.font)
              .style('fill', (d) => fill(d.text.toLowerCase()))
              .text(d => d.text);
          })
          .start();
      }
    };
  });