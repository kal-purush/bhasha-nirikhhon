import { StoreService, Expense } from '../store/store.service';

/** @ngInject */
export function scatterChart(): angular.IDirective {

  return {
    restrict: 'E',
    replace: false,
    scope: {},
    link: (scope: angular.IScope, element: angular.IAugmentedJQuery, attrs: angular.IAttributes) => {
      let margin = {
        top: 0,
        right: 0,
        bottom: 0,
        left: 0
      };
      let width = 640 - margin.left - margin.right;
      let height = 300 - margin.top - margin.bottom;
      let padding = 30;
      let render = (data: Expense[]) => {

        // create scale functions
        let xScale = d3.time.scale()
          .domain([
            new Date(data[0].id),
            d3.time.day.offset(new Date(data[data.length - 1].id), 1)
          ])
          .range([0, width]);

        let yScale = d3.scale.linear()
          .domain([
            d3.min(data, (d: Expense) => { return new Date(d.id).getHours(); }) - 1,
            d3.max(data, (d: Expense) => { return new Date(d.id).getHours(); }) + 1
          ])
          .rangeRound([height, 0]);

        let rScale = d3.scale.linear()
								 .domain([0, d3.max(data, (d: Expense) => { return d.amount; })])
								 .range([5, 20]);

        let color = d3.scale.ordinal().range([
          'rgba(50, 100, 200, 0.9)',
          'rgba(50, 100, 200, 0.7)',
          'rgba(50, 100, 200, 0.5)',
          'rgba(50, 100, 200, 0.3)',
          'rgba(50, 100, 200, 0.1)'
        ]);

        // define x axis
        let xAxis = d3.svg.axis()
          .scale(xScale)
          .orient('bottom')
          .ticks(d3.time.days, 1)
          .tickFormat(d3.time.format('%d'))
          .tickSize(1)
          .tickPadding(10);

        // define y axis
        let yAxis = d3.svg.axis()
          .scale(yScale)
          .orient('left')
          .tickSize(1)
          .tickPadding(5);

        // create svg
        let svg = d3.select(element[0]).append('svg')
          .attr('class', 'scatter-chart')
          .attr('width', width)
          .attr('height', height + padding)
          .append('g')
          .attr('transform', 'translate(' + 40 + ', ' + 5 + ')');

        // create circles
        svg.selectAll('circle')
          .data(data)
          .enter()
          .append('circle')
          .attr('cx', (d: Expense) => {
              return xScale(new Date(d.id));
          })
          .attr('cy', (d: Expense) => {
              return yScale(new Date(d.id).getHours());
          })
          .attr('r', (d: Expense) => {
              return rScale(d.amount);
          })
          .style('fill', (d: Expense) => { return color(d.amount); });

          // create X axis
          svg.append('g')
            .attr('class', 'axis')
            .attr('transform', 'translate(0,' + height + ')')
            .call(xAxis);

          // create Y axis
          svg.append('g')
            .attr('class', 'axis')
            .attr('transform', 'translate(' + 0 + ', 0)')
            .call(yAxis);
      };

      scope.$watch('expenses', function(newValue: Expense[], oldValue: Expense[]) {
        return render(newValue);
      }, true);

    },
    /** @ngInject */
    controller: ($scope: angular.IScope, store: StoreService) => {
      $scope.expenses = store.expenses;
    }
  };
}