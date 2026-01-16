// Function to build metadata panel, bar chart, and bubble chart
function buildDashboard(sample) {
  d3.json("https://static.bc-edx.com/data/dl-1-2/m14/lms/starter/samples.json").then((data) => {
      let samples = data.samples;
      let metadata = data.metadata;
      
      // Filter sample and metadata
      let selectedSample = samples.find(s => s.id === sample);
      let selectedMetadata = metadata.find(m => m.id == sample);
      
      // Update metadata panel
      let panel = d3.select("#sample-metadata");
      panel.html("");
      
      // Loop through each key-value pair and append to panel
      Object.entries(selectedMetadata).forEach(([key, value]) => {
          panel.append("h6").text(`${key.toUpperCase()}: ${value}`);
      });

      // Data for Bar Chart (Top 10 OTUs)
      let otu_ids_bar = selectedSample.otu_ids.slice(0, 10).map(id => `OTU ${id}`).reverse();
      let sample_values_bar = selectedSample.sample_values.slice(0, 10).reverse();
      let otu_labels_bar = selectedSample.otu_labels.slice(0, 10).reverse();

      let barTrace = {
          x: sample_values_bar,
          y: otu_ids_bar,
          text: otu_labels_bar,
          type: "bar",
          orientation: "h"
      };

      let barLayout = {
          title: "Top 10 OTUs Found",
          margin: { l: 100, r: 100, t: 30, b: 30 }
      };

      Plotly.newPlot("bar", [barTrace], barLayout);

      // Data for Bubble Chart
      let bubbleTrace = {
          x: selectedSample.otu_ids,
          y: selectedSample.sample_values,
          text: selectedSample.otu_labels,
          mode: "markers",
          marker: {
              size: selectedSample.sample_values,
              color: selectedSample.otu_ids,
              colorscale: "Earth"
          }
      };

      let bubbleLayout = {
          title: "Bacteria Cultures Per Sample",
          xaxis: { title: "OTU ID" },
          yaxis: { title: "Sample Values" },
          margin: { t: 30 }
      };

      Plotly.newPlot("bubble", [bubbleTrace], bubbleLayout);
  });
}

// Function to initialize the dashboard
function init() {
  d3.json("https://static.bc-edx.com/data/dl-1-2/m14/lms/starter/samples.json").then((data) => {
      let dropdown = d3.select("#selDataset");
      data.names.forEach(name => {
          dropdown.append("option").text(name).property("value", name);
      });

      let firstSample = data.names[0];
      buildDashboard(firstSample);
  });
}

// Function to update dashboard when dropdown changes
function optionChanged(newSample) {
  buildDashboard(newSample);
}

// Initialize dashboard
init();