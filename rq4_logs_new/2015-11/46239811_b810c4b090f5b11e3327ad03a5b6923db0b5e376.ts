module moddynBlog {
  'use strict';

  export class d3HelperService {
    public posts: any = {};
    
    /** @ngInject */
    constructor() {
      
    }

    public removeSVGOnDestroy(svg: any, d3: any) {
      svg.selectAll("*").remove();
      d3.select("svg").remove();
    }

    public createLegend(svg: any, data: any, width: number, legendRectWidth: number, legendRectHeight: number, legendSpacing: number, globalColorScale: any) {
      var legend = svg.selectAll('.legend')
        .data(data)
        .enter()
        .append('g')
        .attr('class', 'legend')
        .attr('transform', function(d, i) {
          return "translate(" + (width+10) + "," + i * 20 + ")";
      });

      legend.append('rect')
        .attr('width', legendRectWidth)
        .attr('height', legendRectHeight)
        .style('fill', function(d, i) { return globalColorScale(i) })
        .style('stroke', function(d, i) { return globalColorScale(i) });

      legend.append('text')
        .attr('x', legendRectWidth + legendSpacing)
        .attr('y', legendRectHeight - legendSpacing)
        .text(function(d) { return d.company; });
    }

  }
}