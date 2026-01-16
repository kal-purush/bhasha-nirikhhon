// Use d3 to read the JSON file.
// The data from the JSON file is arbitrarily named importedData as the argument.
d3.json("data/data.json").then((importedData) => {
  // console.log(importedData);
  let data = importedData;

  // Sort the data array by using the miles value.
  data.sort(function(a, b) {
    return parseFloat(b.miles) - parseFloat(a.miles);
  });

  // Slice the first 10 objects for plotting.
  data = data.slice(0, 10);

  // Reverse the array because of the Plotly defaults.
  data = data.reverse();

  // Trace1 for the car miles data.
  let trace1 = {
    x: data.map(row => row.miles),
    y: data.map(row => row.license_plate),
    text: data.map(row => row.license_plate),
    name: "Car Miles",
    type: "bar",
    orientation: "h"
  };

  // Data
  let chartData = [trace1];

  // Apply the group bar mode to the layout.
  let layout = {
    title: "Car Miles for Employee Used Cars",
    margin: {
      l: 100,
      r: 100,
      t: 100,
      b: 100
    }
  };

  // Render the plot to the div tag with the id of "plot".
  Plotly.newPlot("plot", chartData, layout);
});