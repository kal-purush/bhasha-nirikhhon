//Path for the data 
file_to_load = "Resources/samples.json";
function init() {
  // Grab a reference to the dropdown select element
  var selector = d3.select("#selDataset");

  // Use the list of sample names to populate the select options
  d3.json(file_to_load).then((data) => {
    var sampleNames = data.names;

    sampleNames.forEach((sample) => {
      selector
        .append("option")
        .text(sample)
        .property("value", sample);
    });

    // Use the first sample from the list to build the initial plots
    var firstSample = sampleNames[0];
    buildCharts(firstSample);
    buildMetadata(firstSample);
  });
}

// Initialize the dashboard
init();

function optionChanged(newSample) {
  // Fetch new data each time a new sample is selected
  buildMetadata(newSample);
  buildCharts(newSample);
  
}

// Demographics Panel 
function buildMetadata(sample) {
  d3.json(file_to_load).then((data) => {
    var metadata = data.metadata;
    // Filter the data for the object with the desired sample number
    var resultArray = metadata.filter(sampleObj => sampleObj.id == sample);
    var result = resultArray[0];
    // Use d3 to select the panel with id of `#sample-metadata`
    var PANEL = d3.select("#sample-metadata");

    // Use `.html("") to clear any existing metadata
    PANEL.html("");

    // Use `Object.entries` to add each key and value pair to the panel
    // Hint: Inside the loop, you will need to use d3 to append new
    // tags for each key-value in the metadata.
    Object.entries(result).forEach(([key, value]) => {
      PANEL.append("h6").text(`${key.toUpperCase()}: ${value}`);
    });

  });
}

// 1. Create the buildCharts function.
function buildCharts(sample) {
  // 2. Use d3.json to load and retrieve the samples.json file 
  d3.json(file_to_load).then((data) => {
    // 3. Create a variable that holds the samples array. 
    var samples = data.samples;
    // 4. Create a variable that filters the samples for the object with the desired sample number.
    var resultArray = samples.filter(sampleObj => sampleObj.id == sample);
    //  5. Create a variable that holds the first sample in the array.
    var result = resultArray[0];

    // To print the results
    console.log(`result: ${result}`);

    // 6. Create variables that hold the otu_ids, otu_labels, and sample_values.
    var otuIds = result.otu_ids;
    var otuLabels = result.otu_labels;
    var sampleValues = result.sample_values;

    // To print the results
    // console.log(`otuIds: ${otuIds}`);
    // console.log(`otuLabels: ${otuLabels}`);
    // console.log(`sampleValues: ${sampleValues}`);
    
    
    // 7. Create the yticks for the bar chart.
    // Hint: Get the the top 10 otu_ids and map them in descending order  
    //  so the otu_ids with the most bacteria are last. 
    
    var yticks = otuIds.map(element => {return`OTU ID: ${element}`}).slice(0,10).reverse();
    

    // 8. Create the trace for the bar chart. 
    var barData = [{
      x: sampleValues.slice(0,10).reverse(),
      y: yticks,
      text: otuLabels.slice(0,10).reverse(),
      type: "bar",
      orientation: "h"
    }];
    // 9. Create the layout for the bar chart. 
    var barLayout = {
      title: "Top 10 bacteria cultures found",
      margin: {
        l: 100, b: 40
      },
      paper_bgcolor:"#fff8f3"


              
    };
    // 10. Use Plotly to plot the data with the layout. 
    Plotly.newPlot("bar", barData, barLayout);

    // 1. Create the trace for the bubble chart.
    var bubbleData = [{
      x: otuIds,
      y: sampleValues,
      text: otuLabels,
      mode: 'markers',
      marker: {
      size: sampleValues,
      color: otuIds,
      colorscale: "Earth"
      
      }
    }
   
    ];

    // 2. Create the layout for the bubble chart.
    var bubbleLayout = {
      title: 'Bacteria Cultures Per Sample',
      xaxis: {title: "OTU ID"},
      automargin: true,
      hovermode: "closest",
      paper_bgcolor:"#fff8f3"
      
    };

    // 3. Use Plotly to plot the data with the layout.
    Plotly.newPlot("bubble", bubbleData, bubbleLayout ); 

    // Create a variable that holds the metadata array. 
    var metadata = data.metadata;

    // Create a variable that  filters the metadata array for the object with the desired sample number.
  var metaArray = metadata.filter(sampleObj => sampleObj.id == sample);

    //  Create a variable that holds the first metadata in the array.
    var metaResult = metaArray[0];
    console.log(`metaResult: ${metaResult}`);

    // create a variable that converts the washing frequency to a floating point number.
    var wFreq = parseFloat(metaResult.wfreq);
    console.log(`washing frequency: ${wFreq}`);

    // 4. Create the trace for the gauge chart.
    var gaugeData = [{
      
      value: wFreq,
      title: { text: "<b>Belly Button Washing Frequency</b> <br> Scrubs per week", font: {size: 16}}, 
      type: "indicator",
      mode: "gauge+number",
      gauge: {
        axis:{range: [null,10], dtick: "2"},
        bar: {color: "black"},
        steps: [
          { range: [0, 2], color: "red" },
          { range: [2, 4], color: "orange" }, 
          { range: [4, 6], color: "yellow" },
          { range: [6, 8], color: "greenyellow" },
          { range: [8, 10], color: "green" },
        ]
      }
    
    }
      
    ];
    
    // 5. Create the layout for the gauge chart.
    var gaugeLayout = { 
     automargin: true,
     paper_bgcolor:"#fff8f3"
               
    };


    // 6. Use Plotly to plot the gauge data and layout.
    Plotly.newPlot("gauge", gaugeData, gaugeLayout);

    
  });
}