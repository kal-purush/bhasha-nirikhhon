// @TODO: YOUR CODE HERE!
// Pull in data from CSV file
// build scatter plot
function buildScatterChart() {

    // set the dimensions and margins of the graph
    var margin = { top: 10, right: 30, bottom: 30, left: 60 },
        width = 760 - margin.left - margin.right,
        height = 600 - margin.top - margin.bottom;

    // append the svg object to the body of the page
    var svg = d3.select("#scatter")
        .append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform",
            "translate(" + margin.left + "," + margin.top + ")");

    // Pull in the CSV data
    let csvFile = "/Homework/assets/data/data.csv";
    d3.csv(csvFile).then((data) => {

        // Add X axis
        var x = d3.scaleLinear()
            .domain([30, 46])
            .range([0, width]);
        svg.append("g")
            .attr("transform", "translate(0," + height + ")")
            .call(d3.axisBottom(x));

        // Add Y axis
        var y = d3.scaleLinear()
            .domain([35000, 75000])
            .range([height, 0]);
        svg.append("g")
            .call(d3.axisLeft(y));

        // Add dots
        svg.append("g")
            .selectAll("circle")
            .data(data)
            .enter()
            .append("circle")
            .attr("cx", (d => x(d.age) ))
            .attr("cy", (d => y(d.income) ))
            .attr("r", 15 )                            // (d => (d.poverty) )
            .style("fill", "#69b3a2")

        svg.append("g")
            .selectAll("text")
            .data(data)
            .enter()
            .append("text")
            .text(d => d.abbr)
            .attr("font-size", 12)
            .attr("dx", (d => x(d.age) - 6 ))
            .attr("dy", (d => y(d.income) + 6 ))
            .style("fill", "white");


        svg.append("text")
            .attr("x", (-height / 2))
            .attr("y", -42)
            .attr("transform", "rotate(-90)")
            .attr("font-size", 18)
            .text("Age");

        svg.append("text")
            .attr("x", ((width / 2) + 10 ))
            .attr("y", height + 20)
            .attr("font-size", 18)
            .text("Income")
        
    });
}



function init() {
    // Pull in the CSV data
    let csvFile = "/Homework/assets/data/data.csv";
    d3.csv(csvFile).then((data) => {

        // Set a default to build the initial plots
        buildScatterChart(data);
        // console.log(data);
    });
};


// Initialize the dashboard
init();