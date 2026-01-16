import { StoreService, Account, IExpensioScope } from '../store/store.service';

interface IGaugeChartScope extends IExpensioScope {
  account: string;
}

/** @ngInject */
export function gaugeChart(): angular.IDirective {

  return {
    restrict: 'E',
    replace: false,
    scope: {
      account: '@'
    },
    link: (scope: IGaugeChartScope, element: angular.IAugmentedJQuery, attrs: angular.IAttributes) => {
      let width = 200;
      let height = 160;
      let radius = Math.min(width, height) / 2;
      let barWidth = 20 * width / 300;

      let arcColorFn = d3.interpolateHsl(d3.rgb('#FD0'), d3.rgb('#D55'));

      let padRad = 0.025;
      let chartInset = 10;

      // orientation of gauge, start at 270deg
      let totalPercent = 0.75;

      // utitily function
      let degreeToRadian = (degree: number) => {
        return degree * Math.PI / 180;
      };

      let percentageToDegree = (percent: number) => {
        return percent * 360;
      };

      let percentageToRadian = (percent: number) => {
        return degreeToRadian(percentageToDegree(percent));
      };
      // ----------------

      let render = (data: Account[]) => {

        let currentAccount = data.filter((account: Account) => {
          return account.name === scope.account;
        })[0];

        let percentData = 1 - (((currentAccount.balance / 100000) * 100) / 100);

        // remove all previous items before render
        d3.select(element[0]).selectAll('*').remove();

        let svg = d3.select(element[0]).append('svg')
          .attr('width', width)
          .attr('height', height / 2);

        // add layer for the panel
        let chart = svg.append('g')
          .attr('transform', 'translate(' + (width / 2) + ', ' + (height / 2) + ')');

        chart.append('path')
          .attr('class', 'chart-filled')
          .attr('fill', () => {
            return arcColorFn(percentData);
          });

        chart.append('path')
          .attr('class', 'chart-empty')
          .style('fill', '#eee');

        let backgroundArc = d3.svg.arc()
          .outerRadius(radius - chartInset)
          .innerRadius(radius - chartInset - barWidth);

        let foregroundArc = d3.svg.arc()
          .outerRadius(radius - chartInset)
          .innerRadius(radius - chartInset - barWidth);

        let nextStart = totalPercent;
        let arcStart = percentageToRadian(nextStart);
        let arcEnd = arcStart + percentageToRadian(percentData);

        nextStart += percentData / 2;

        backgroundArc
          .startAngle(arcStart)
          .endAngle(arcEnd);

        arcStart = percentageToRadian(nextStart);
        arcEnd = arcStart + percentageToRadian((1 - percentData) / 2);

        foregroundArc
          .startAngle(arcStart + padRad)
          .endAngle(arcEnd);

        chart.select('.chart-filled')
          .attr('d', backgroundArc);

        chart.select('.chart-empty')
          .attr('d', foregroundArc);

        // append Text at center of donut (account name)
        svg
          .append('text')
          .attr('text-anchor', 'middle')
          .attr('font-size', '12px')
          .attr('transform', 'translate(' + width / 2 + ',' + ((width / 2) - 20) + ')')
          .text(scope.account);

        // append Text at center of donut (utilization)
        svg
          .append('text')
          .attr('text-anchor', 'middle')
          .attr('font-size', '12px')
          .attr('transform', 'translate(' + width / 2 + ',' + ((width / 2) - 40) + ')')
          .text((percentData * 100).toFixed(2) + '%');

      };

      scope.$watch('accounts', function(newValue: Account[], oldValue: Account[]) {
        return render(newValue);
      }, true);

    },
    /** @ngInject */
    controller: ($scope: IGaugeChartScope, store: StoreService) => {
      $scope.accounts = store.accounts;
    }
  };
}