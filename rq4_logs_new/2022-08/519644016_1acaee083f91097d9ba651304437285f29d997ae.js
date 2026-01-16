let url =
  "https://raw.githubusercontent.com/freeCodeCamp/ProjectReferenceData/master/GDP-data.json";

let req = new XMLHttpRequest();

let data;
let values; //where we store our value data array

let heightScale; //use to determine the height of the bars
let xScale; //use to determine where the bars are placed horizontally on the canvas
let yScale;
let xAxisScale; //for drawing the xAxis
let yAxisScale; //use to create the y axis along the left

let width = 800;
let height = 600;
let padding = 40;

let svg = d3.select("svg");

let drawCanvas = () => {
  svg.attr("width", width);
  svg.attr("height", height);
};

let generateScales = () => {
  //in the domain we specify the lowest value and the highest value of the input
  heightScale = d3
    .scaleLinear()
    .domain([
      0,
      d3.max(values, (item) => {
        return item[1];
      }),
    ])
    .range([0, height - 2 * padding]);
  //create xScale to be able to position our bar along the x axis horizontally
  xScale = d3
    .scaleLinear()
    .domain([0, values.length - 1])
    .range([padding, width - padding]);
  //create a scale for the x axis of Dates
  let datesArray = values.map((item) => {
    return new Date(item[0]);
  });
  xAxisScale = d3
    .scaleTime()
    .domain([d3.min(datesArray), d3.max(datesArray)])
    .range([padding, width - padding]);
  yAxisScale = d3
    .scaleLinear()
    .domain([
      0,
      d3.max(values, (item) => {
        return item[1];
      }),
    ])
    .range([height - padding, padding]);
};

let drawBars = () => {
  //create tooltip
  let tooltip = d3
    .select("body")
    .append("div")
    .attr("id", "tooltip")
    .style("visibility", "hidden")
    .style("width", "auto")
    .style("height", "auto");
  //create rectangle
  svg
    .selectAll("rect")
    .data(values)
    .enter()
    .append("rect")
    .attr("class", "bar")
    .attr("width", (width - 2 * padding) / values.length)
    .attr("data-date", (item) => {
      return item[0];
    })
    .attr("data-gdp", (item) => {
      return item[1];
    })
    .attr("height", (item) => {
      return heightScale(item[1]);
    })
    .attr("x", (item, index) => {
      return xScale(index);
    })
    .attr("y", (item) => {
      return height - padding - heightScale(item[1]);
    })
    .on("mouseover", (item) => {
      tooltip.transition().style("visibility", "visible");

      tooltip.text(item[0]); //work only with d3 version 5 script cdn

      document.querySelector("#tooltip").setAttribute("data-date", item[0]);
    })
    .on("mouseout", (item) => {
      tooltip.transition().style("visibility", "hidden");
    });
};

let generateAxes = () => {
  let xAxis = d3.axisBottom(xAxisScale);
  let yAxis = d3.axisLeft(yAxisScale);

  svg
    .append("g")
    .call(xAxis)
    .attr("id", "x-axis")
    .attr("transform", "translate(0," + (height - padding) + ")"); //we move the axis at the bottom

  svg
    .append("g")
    .call(yAxis)
    .attr("id", "y-axis")
    .attr("transform", "translate(" + padding + ", 0)");
};

//Fetching json data
req.open("GET", url, true);
req.onload = () => {
  //if have a response
  //   console.log(req.responseText);
  data = JSON.parse(req.responseText); //convert response from json format to javascript object
  values = data.data;
  console.log(values);
  drawCanvas();
  generateScales();
  drawBars();
  generateAxes();
};
req.send();