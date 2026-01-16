import { StoreService } from '../store/store.service';

interface IDonutChartScope extends angular.IScope {
  data: number[];
  payment: string;
}

/** @ngInject */
export function donutChart(): angular.IDirective {

  return {
    restrict: 'E',
    replace: false,
    scope: {
      payment: '@'
    },
    link: (scope: IDonutChartScope, element: angular.IAugmentedJQuery, attrs: angular.IAttributes) => {
      var data = scope.data;

      var color = d3.scale.ordinal().range([
        'rgba(50, 100, 200, 0.9)',
        'rgba(50, 100, 200, 0.7)',
        'rgba(50, 100, 200, 0.5)',
        'rgba(50, 100, 200, 0.3)',
        'rgba(50, 100, 200, 0.1)'
        ]);
      var outerR = 100;
      var innerR = 70;
      var fontSize = 12 * outerR / 100;

      // creates the Chart
      var canvas = d3.select(element[0])
        .append('svg')
        .attr('width', outerR * 2)
        .attr('height', outerR * 2); // creates the paintable canvas

      var group = canvas.append('g').attr('transform', 'translate(' + outerR + ',' + outerR + ')'); // sets the location
      var arc = d3.svg.arc().innerRadius(innerR).outerRadius(outerR); // creates the donut look
      var pie = d3.layout.pie().value((d) => {
        return d.amount;
      });
      var arcs = group.selectAll('.arc')
        .data( pie(data) )
        .enter()
        .append('g')
        .attr('class', 'arc');

      arcs.append('path')
        .attr('d', arc)
        .attr('fill', (d) => { return color(d.data.amount); });

      arcs.append('text')
        .attr('transform', (d) => { return 'translate(' + arc.centroid(d) + ')'; })
        .attr('text-anchor', 'middle')
        .attr('font-size', fontSize + 'px')
        .text((d) => { return d.data.category; });

      // append Text at center of donut
      canvas
        .append('text')
        .attr('text-anchor', 'middle')
        .attr('font-size', fontSize + 'px')
        .attr('transform', 'translate(' + outerR + ',' + outerR + ')')
        .text((d) => { return scope.payment; });
    },
    /** @ngInject */
    controller: ($scope: IDonutChartScope, store: StoreService) => {
      let categories = store.expenses.filter((expense) => {
        return expense.paymentMode === $scope.payment;
      }).groupBy('category');

      let data = [];

      for (let category in categories) {
        if (categories.hasOwnProperty(category)) {
          let totalAmount = categories[category].reduce((acc, expense) => {
            return acc = acc + expense.amount;
          }, 0);

          data.push({
            category: category,
            amount: totalAmount
          });
        }
      }

      $scope.data = data;
    }
  };

}