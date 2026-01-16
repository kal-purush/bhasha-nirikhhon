
/**
 * Created by michaelevan on 6/22/17.
 */

"use strict";
/*//// Coding a chart automatically WITH SVG ////*/

var data = [
  {name: "Locke",    value:  4},
  {name: "Reyes",    value:  8},
  {name: "Ford",     value: 15},
  {name: "Jarrah",   value: 16},
  {name: "Shephard", value: 23},
  {name: "Kwon",     value: 42}
];

var width = 420,
    barHeight = 20;

var x = d3.scale.linear()
    .range([0, width]);

var chart = d3.select(".chart")
    .attr("width", width)
    .attr("height", barHeight * data.length);

// d3.select("data", type, function(error, data) {
//   x.domain([0, d3.max(data, function(d) { return d.value; })]);


  var bar = chart.selectAll("g")
      .data(data)
    .enter().append("g")
      .attr("transform", function(d, i) { return "translate(0," + i * barHeight + ")"; });

  bar.append("rect")
      .attr("width", function(d) { return x(d.value); })
      .attr("height", barHeight - 1);

  bar.append("text")
      .attr("x", function(d) { return x(d.value) - 3; })
      .attr("y", barHeight / 2)
      .attr("dy", ".35em")
      .text(function(d) { return d.value; });
// });

function type(d) {
  d.value = +d.value; // coerce to number
  return d;
}

// function type(d) {
//   d.value = +d.value; // coerce to number
//   return d;
// }


















///// Coding a chart automatically WITHOUT SVG /////

// var data = [4, 8, 15, 16, 23, 42];

// var x = d3.scale.linear()
//     .domain([0, d3.max(data)])
//     .range([0, 420]);
//
//
// // d3.select(".chart")
// //   .selectAll("div")
// //     .data(data)
// //   .enter().append("div")
// //     .style("width", function(d) { return x(d) + "px"; })
// //     .text(function(d) { return d; });
//
// // # Coding a chart automatically.
//
// d3.select(".chart")         // select chart container using class selector
//   .selectAll("div")         // initiate data join by defining selection TO WHICH we join data. Declaring the elements that you want to exist.
//   .data(data)
//   .enter().append("div")
//   .style("width", function(d) { return d * 10 + "px"; })
//   .text(function(d) { return d; });