function plot(id){
  d3.json("samples.json").then(data => {

    console.log(data);

var reference = data.samples.filter(sample => sample.id === id)[0];

    console.log(reference);

    var samplevalues = reference.sample_values.slice(0, 10).reverse();

    console.log(samplevalues);

    var otuids = (reference.otu_ids.slice(0,10)).reverse();

    console.log(otuids);

    var trace1 = {
      x: sampleValues,
      y: otuId,
      // text: labels,
      marker: {
          color: 'rgb((142,124,195)'},
      type: "bar",
      orientation: "h"
      };
  
  var data = [trace1];
  
  // layout for bar chart
  var layout = {
      title: "Top 10 OTU",
      yaxis:{
          tickmode:"linear",
      },
      margin: {
          l: 100,
          r: 100,
          t: 100,
          b: 30
      }
  };

  // plot bar chart
  Plotly.newPlot("bar", data, layout);

  })
}
plot();