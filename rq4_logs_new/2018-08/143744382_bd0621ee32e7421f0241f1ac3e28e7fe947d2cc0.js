import data from './data';
import * as d3 from 'd3';
import { fit, measureWidth, Pack, format } from './utils';

let height = 1800;
let width = 1800;

const pack = new Pack(width, height);
const root = pack(data);
const leaves = root.leaves().filter(d => d.depth && d.value);

// Set color gradient.
const lowColor = '#72DCC4';
const highColor = '#199277';

// Text colors.
const darkBG = "#ffffff";
const lightBG = "#333333";
const fillColor = "#dddddd";

// Calculate continuous gradient value.
const gradient = d3.scaleLinear().domain([0, 18]).range([lowColor, highColor]);

const bubbleChart = (el) => {
  const svg = d3.select(el).append('svg')
      .attr('viewBox', `0 0 ${width} ${height}`)
      .style("background", "#fff")
      .style("width", "100%")
      .style("height", "auto");

  svg.append("g")
      .attr("fill", fillColor)
    .selectAll("circle")
    .data(leaves)
    .enter().append("circle")
      .attr("cx", d => d.x)
      .attr("cy", d => d.y)
      .attr("r", d => d.r)
      .attr("fill", function(d) { return gradient(d.data.value); })
    .append("title")
        .text(d => `Keyword: \"${d.data.name}\"\nFrequency: ${format(d.data.value)}`);

  svg.append("g")
      .style("font", "10px sans-serif")
      .attr("text-anchor", "middle")
      .attr("pointer-events", "none")
    .selectAll("text")
    .data(leaves)
    .enter().append("text")
      // Set text color depending on background.
      .attr("fill", d => d3.lab(d.data.color).l < 60 ? lightBG : darkBG)
      .attr("transform", d => {
        const {lines, radius} = fit(d.data.name);
        console.log(lines, radius, d.r);
        d.lines = lines;
        return `translate(${d.x},${d.y}) scale(${Math.min(3, 0.9 * d.r / radius)})`;
      })
      // .attr("opacity", function(d) {
      //   if (d.data.value > 4) {
      //     return 1;
      //   } else {
      //     return 0;
      //   }
      // })
    .selectAll("tspan")
    .data(d => d.lines)
    .enter().append("tspan")
      .attr("x", 0)
      .attr("y", (d, i, data) => (i - data.length / 2 + 0.8) * 12)
      .text(d => d.text)
      .attr("font-family", "Circular")
}

export default bubbleChart;