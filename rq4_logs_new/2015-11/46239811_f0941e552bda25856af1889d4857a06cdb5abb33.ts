module moddynBlog {
  'use strict';
  
  /** @ngInject */
  export function milkCanadaAllPerYearBar(): ng.IDirective {

    return {
      restrict: 'E',
      scope: {
        data: '='
      },
      controller: MilkCanadaController,
      controllerAs: 'mcc',
      link: (scope: any, ele: JQuery, attrs: ng.IAttributes) => {
        scope.mcc.d3Service.d3().then(d3 => {
          scope.mcc.d3HelperService.getData('/assets/data/milk/Canada_All_Per_Year_No_Total.json').then((data: any) => {
            var margin = { top: 20, right: 20, bottom: 30, left: 40 },
              width = 1260 - margin.left - margin.right,
              height = 500 - margin.top - margin.bottom;

            var parseDate = d3.time.format("%Y").parse;

            scope.mcc.d3BarChartHelperService.createBarChart(d3, width, height, margin, data, ele, 'line-canada', "Canada Milk & Cream Consumption Per Year", parseDate);

          });
        });
      },
      bindToController: true
    };
  }

  /** @ngInject */
  class MilkCanadaController {

    constructor(private d3Service: any, private d3BarChartHelperService: any, private d3HelperService: any) {
    }
  }
}