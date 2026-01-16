function drawAccessBars(){
  async function readAndDraw(){
    // set the dimensions and margins of the graph
    var margin = {top: 60, right: 30, bottom: 100, left: 70},
        width = 620 - margin.left - margin.right,
        height = 400 - margin.top - margin.bottom;

    // append the svg object to the body of the page
    var svg = d3.select("#accessBars")
      .select("svg")
      .append("g")
        .attr("transform",
              "translate(" + margin.left + "," + margin.top + ")");

    svg.append('text')
      .text('Access to Electricity, Water and Cold Storage for Butcher shops')
      .attr('x', 0)
      .attr('y', -40)
      .style('text-anchor', 'start')
      .style('font-size', '18px');

    // Parse the Data
    let data = await d3.csv("Data/BSAccess.csv");

      // List of subgroups = header of the csv files = soil condition here
      var subgroups = data.columns.slice(1)

      // List of groups = species here = value of the first column called group -> I show them on the X axis
      var groups = d3.map(data, function(d){return(d.group)}).keys()

      // Add X axis
      var x = d3.scaleBand()
          .domain(groups)
          .range([0, width])
          .padding([0.2])
      const xAxis = svg.append("g")
        .attr('class', 'axis x')
        .attr("transform", "translate(0," + height + ")")
        .call(d3.axisBottom(x).tickSize(0));

      // Add Y axis
      var y = d3.scaleLinear()
        .domain([0, 100])
        .range([ height, 0 ]);

      const yAxis = svg.append("g")
        .attr('class', 'axis y')
        .call(d3.axisLeft(y).ticks(4));

      const xAxisLab = xAxis.append('text')
          .text('District')
          .attr('x', width/2)
          .attr('y', '30px')
          .style('fill', 'black')
          .style('font-size', '14px')
          .style('text-anchor', 'middle');

      const yAxisLab = yAxis.append('text')
          .text('Percent')
          .attr('x', -height)
          .attr('y', '-45px')
          .style('fill', 'black')
          .style('font-size', '12px')
          .style('transform', 'rotate(-90deg)')
          .style('text-anchor', 'start');

        // add the Y gridlines
      const yGrid = svg.append("g")
          .attr("class", "grid")
          .call(d3.axisLeft(y).ticks(4).tickSize(-width).tickFormat(""))

      xAxis.selectAll('text')
          .attr('dy', '1.3em');

      // Another scale for subgroup position?
      var xSubgroup = d3.scaleBand()
        .domain(subgroups)
        .range([0, x.bandwidth()])
        .padding([0.05])

      // color palette = one color per subgroup
      var color = d3.scaleOrdinal()
        .domain(subgroups)
        .range(['#a6cee3', '#1f78b4', '#b2df8a'])

      // Show the bars
      svg.append("g")
        .selectAll("g")
        // Enter in data = loop group per group
        .data(data)
        .enter()
        .append("g")
          .attr("transform", function(d) { return "translate(" + x(d.group) + ",0)"; })
        .selectAll("rect")
        .data(function(d) { return subgroups.map(function(key) { return {key: key, value: d[key]}; }); })
        .enter().append("rect")
          .attr("x", function(d) { return xSubgroup(d.key); })
          .attr("y", height)
          .attr("width", xSubgroup.bandwidth())
          .attr("height", 0)
          // .attr("rx", '2px')
          // .attr("ry", '2px')
          .attr("fill", function(d) { return color(d.key); })
          .transition()
          .duration(1250)
          .attr("y", function(d) { return y(d.value); })
          .attr("height", function(d) { return height - y(d.value); });

    var ordinal = d3.scaleOrdinal()
      .domain(["Electricity", "Water", "Cold Storage"])
      .range(['#a6cee3', '#1f78b4', '#b2df8a']);

    svg.append("g")
      .attr("class", "legendOrdinal")
      .attr("transform", "translate(400, 280)");

    var legendOrdinal = d3.legendColor()
      //d3 symbol creates a path-string, for example
      //"M0,-8.059274488676564L9.306048591020996,
      //8.059274488676564 -9.306048591020996,8.059274488676564Z"
      .shapePadding(0)
      .scale(ordinal);
    //
    svg.select(".legendOrdinal")
      .call(legendOrdinal);
  }

  readAndDraw();
}

drawAccessBars();