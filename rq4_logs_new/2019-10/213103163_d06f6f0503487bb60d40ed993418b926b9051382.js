// Set up our chart
var svgWidth = window.innerWidth;
var svgHeight = window.innerHeight;

var margin = {
	top: 60,
	right: 40,
	bottom: 100,
	left: 100
};

var chartWidth = svgWidth - margin.left - margin.right;
var chartHeight = svgHeight - margin.top - margin.bottom;

// Create an SVG wrapper, append an SVG group that will hold our chart,
// and shift the latter by left and top margins.
var svg = d3
	.select('.chart')
	.append("svg")
	.attr("width", svgWidth)
	.attr("height", svgHeight)
	.append("g")

// Append group element
	var chart = svg.append("g")
	.attr("transform", `translate(${margin.left}, ${margin.top})`);

  d3.select("body")
	 .append("div")
	 .attr("class", "tooltip")
	 .style("opacity", 1)

// Import data from the data.csv file, call the function healthData
  d3.csv("data.csv", function(error, healthData) {
	if (error) throw error;

 // Parse data
  healthData.forEach(function(data) {
	  data.poverty = +data.poverty;
	  data.healthStatus = +data.healthStatus;
  });

// Create the scales for the chart
	var x = d3.scaleLinear().range([0, chartWidth]);
	var y = d3.scaleLinear().range([chartHeight, 0]);

	var bottomAxis = d3.axisBottom(x);
	var leftAxis = d3.axisLeft(y);

// Scale the range of the data
	x.domain([0,d3.max(healthData, function(data){
		return +data.poverty;
	})]);

	y.domain([0,d3.max(healthData, function(data){
		return +data.healthStatus;
	})]);

// Defining tooltip
	var toolTip = d3.tip()
		.attr("class", "toolTip")
		.offset([80,-60])
		.html(function(data){
			var state = data.state;
			var povertyRate = +data.poverty;
			var healthStatus = +data.healthStatus;
			return(state + "<br> Poverty Rate (%): " + povertyRate + "<br> Health Rate (%): " + healthStatus)
		});

	chart.call(toolTip);

// Defining the circles on the chart
	chart.selectAll("circle")
		.data(healthData)
		.enter().append("circle")
			.attr("cx", function(data, index){
				console.log(data.poverty);
				return x(data.poverty);
			})
			.attr("cy", function(data, index){
				console.log(data.healthStatus);
				return y(data.healthStatus);
			})
			.attr('r', "10")
			.attr("fill", "blue")
			.style("opacity", 0.5)
			.on("click", function(data){
				toolTip.show(data);
			});

// Add the x-axis
	chart.append("g")
		.attr("transform", `translate(0, ${chartHeight})`)
		.call(bottomAxis);

// Add the y-axis
	chart.append('g')
		.call(leftAxis);

// Text for the y-axis
	chart.append("text")
		.attr("transform", "rotate(-90)")
		.attr("y", 0 - margin.left + 40)
		.attr("x", 0 - (chartHeight))
		.attr("dy", "1em")
		.attr("class", "axisText")
		.style("text-anchor", "margintop")
		.text("Population in Fair or Poor Health (%)")

// Text for the x-axis
	chart.append("text")
		.attr("transform", "translate(" + (chartWidth/2) + ", " + (chartHeight + margin.top + 20) + ")")
		.attr("class", "axisText")
		.style("text-anchor", "middle")
		.text("Population Below the Poverty Line (%)");

// Text for title
 	chart.append("text")
		.style("text-anchor", "right")
		.attr("class", "axisText")
		.text("Correlation of Health vs. Poverty in USA");
})