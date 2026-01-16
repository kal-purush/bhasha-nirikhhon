// Load data from a JSON endpoint asynchronously using D3.js
d3.json("https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.1/14-Interactive-Web-Visualizations/02-Homework/samples.json").then((data) => {
    // Populate the dropdown menu with sample names
    var select = d3.select("#selDataset");
    data.names.forEach((name) => {
      select.append("option").text(name).property("value", name);
    });

    // Initialize the dashboard with the first sample's data
    var firstSample = data.names[0];
    updateCharts(firstSample, data);
    updateMetadata(firstSample, data);
});

// Function to update the charts based on the selected sample
function updateCharts(sample, data) {
    var samples = data.samples;
    var resultArray = samples.filter(sampleObj => sampleObj.id == sample);
    var result = resultArray[0];
  
    // Extract data for plotting
    var otu_ids = result.otu_ids;
    var otu_labels = result.otu_labels;
    var sample_values = result.sample_values;
  
    // Configure and plot the bar chart for top 10 bacteria cultures found
    var barData = [{
      x: sample_values.slice(0, 10).reverse(),
      y: otu_ids.slice(0, 10).map(otuID => `OTU ${otuID}`).reverse(),
      text: otu_labels.slice(0, 10).reverse(),
      type: "bar",
      orientation: "h"
    }];
  
    var barLayout = {
      title: "Top 10 Bacteria Cultures Found",
      margin: { t: 30, l: 150 }
    };
  
    Plotly.newPlot("bar", barData, barLayout);
  
    // Configure and plot the bubble chart for bacteria cultures per sample
    var bubbleLayout = {
      title: "Bacteria Cultures Per Sample",
      hovermode: "closest",
      xaxis: { title: "OTU ID" },
      margin: { t: 30 }
    };
    var bubbleData = [{
      x: otu_ids,
      y: sample_values,
      text: otu_labels,
      mode: "markers",
      marker: {
        size: sample_values,
        color: otu_ids,
        colorscale: "Earth"
      }
    }];
  
    Plotly.newPlot("bubble", bubbleData, bubbleLayout);
}

// Function to update the sample metadata information
function updateMetadata(sample, data) {
    var metadata = data.metadata;
    var resultArray = metadata.filter(sampleObj => sampleObj.id == sample);
    var result = resultArray[0];
    var PANEL = d3.select("#sample-metadata");
  
    PANEL.html(""); // Clear any existing metadata
    // Display each metadata entry as a separate paragraph
    Object.entries(result).forEach(([key, value]) => {
      PANEL.append("h6").text(`${key.toUpperCase()}: ${value}`);
    });
}

// Function to handle a change in the selected sample
function optionChanged(newSample) {
    // Reload data and update the dashboard for the new sample
    d3.json("https://2u-data-curriculum-team.s3.amazonaws.com/dataviz-classroom/v1.1/14-Interactive-Web-Visualizations/02-Homework/samples.json").then((data) => {
      updateCharts(newSample, data);
      updateMetadata(newSample, data);
    });
}