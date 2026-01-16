module moddynBlog {
  'use strict';

  export class d3BarChartHelperService {
    
    /** @ngInject */
    constructor() {}

    public createSVG(d3, width, height, margin, data, ele) {
      var svg = d3.select(ele[0]).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
          .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
      return svg;
    }

    public createXAxis(d3, x) {
      var xAxis = d3.svg.axis()
        .scale(x)
        .orient("top")
        .tickFormat(function(d) {
          return "";
        });
      return xAxis;
    }

    public createYAxis(d3, y) {
      var yAxis = d3.svg.axis()
        .scale(y)
        .orient("left");
      return yAxis;
    }

    public fillAxisWithData(d3, x, y, data) {
      x.domain(data.map(function(d) { return d.company; }));
      y.domain([0, d3.max(data, function(d) { return d.avgOrgDepth; })]);
    }

    public createBarGroupAndTranslate(svg, height, xAxis) {
      svg.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxis);
    }

    public addDataToBars(svg, data, x, y, height, globalColorScale) {
      svg.selectAll(".bar")
        .data(data)
        .enter().append("rect")
          .attr("class", "bar")
          .attr("x", function(d) { return x(d.company); })
          .attr("width", x.rangeBand())
          .attr("y", function(d) { return y(d.avgOrgDepth); })
          .style('fill', function(d, i) { return globalColorScale(i) })
          .attr("height", function(d) { return height - y(d.avgOrgDepth); });
    }

    public createDataLabels(svg, data, width, x, y) {
      var dataLabels = svg.selectAll(".bartext")
        .data(data)
        .enter()
        .append("text")
        .text(function(d) {return d.avgOrgDepth})
        .attr("x", function(d, i) {
          return i * (width / data.length) + (width / data.length - 1) / 2;
        })
        .attr("y", function(d, i) {
          return y(d.avgOrgDepth) + 15;
        })
        .attr("font-family", "sans-serif")
        .attr("font-size", "11px")
        .attr("fill", "white")
        .attr("text-anchor", "middle");
    }

    public createLegend(svg: any, data: any, width: number, globalColorScale: any, legendRectWidth: number = 14, legendRectHeight: number = 14, legendSpacing: number = 4) {
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

    public createTitle(svg: any, width: number, margin: any, title: string) {
      svg.append("text")
        .attr("x", (width / 2))             
        .attr("y", 0 - (margin.top / 2))
        .attr("text-anchor", "middle")  
        .style("font-size", "16px") 
        .style("text-decoration", "underline")  
        .text(title);
    }

  }
}