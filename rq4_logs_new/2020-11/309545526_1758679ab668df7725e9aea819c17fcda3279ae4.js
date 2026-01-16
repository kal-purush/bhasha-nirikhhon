//Chart Set up
var svgWidth = 1100;
var svgHeight = 700;

var margin = {
  top: 50,
  right: 60,
  bottom: 80,
  left: 120
};

var height = svgHeight - margin.top - margin.bottom;
var width = svgWidth - margin.left - margin.right;


var svg = d3.select("#scatter")
  .append("svg")
  .attr("height", svgHeight)
  .attr("width", svgWidth)


var chartGroup = svg.append("g")
  .attr("transform", `translate(${margin.right}, ${margin.top})`);

// Importing Data

d3.csv("/assets/data/data.csv").then(function(csv_data) {

// Data Formatting
        csv_data.forEach(function(data) {data.poverty = +data.poverty;
            data.healthcare = +data.healthcare;
  });

  
   // Create scaling functions   
    var xLinearScale = d3.scaleLinear()
        .domain([9, d3.max(csv_data, d => d.poverty)])
        .range([2, width]);

    var yLinearScale = d3.scaleLinear()
        .domain([3, d3.max(csv_data, d => d.healthcare)])
        .range([height, 0]);
 
    // Adding Axis
     var x_axis = d3.axisBottom(xLinearScale);
     var y_axis = d3.axisLeft(yLinearScale); 


    chartGroup.append("g")
        .attr("transform", `translate(0, ${height})`)
        .call(x_axis);
        

    chartGroup.append("g")
        .call(y_axis);

        var circles=chartGroup.selectAll("circle")
        .data(csv_data)
        .enter()
        .append("circle")
        .attr("opacity", ".8")
        .attr("r", "12")
        .attr("fill", "lightblue")
        .attr("cx", d => xLinearScale(d.poverty))
        .attr("cy", d => yLinearScale(d.healthcare))
        
        
      
        chartGroup.selectAll('.stateText')
        .data(csv_data)
        .enter()
        .append('text')
        .attr("opacity", ".95")
        .classed('stateText', true)
        .attr('font-size', '10px')
        .attr('x', d => xLinearScale(d.poverty))
        .attr('y', d => yLinearScale(d.healthcare))
        .text(d => d.abbr)

        

//  Tooltip
    var toolTip = d3.tip()
        .style("background", "blue")
        .attr("class", "tooltip")
        .style("position", "absolute")
        .html(function(d) {
            return (`${d.state}<br>In Poverty (%): ${d.poverty}<br>Lacks Healthcare (%): ${d.healthcare}`)
        });      

        circles.call(toolTip);

       

    axes()

});



function axes() {
     
    chartGroup.append("text")
    .attr("transform", `translate(${width / 1.5}, ${height + margin.top + 15})`)
    .attr("class", "x-axis")
    .attr("text-align", "middle")
    .attr("font-weight", "bold")
    .text("In Poverty (%)");

    chartGroup.append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 0 - margin.left - 3)
        .attr("x", 0 - (height / 1.30))
        .attr("dy", "1em")
        .attr("class", "y-axis")
        .attr("text-align", "center")
        .attr("font-weight", "bold")
        .text("Lacks Healthcare (%)");

  
    
}