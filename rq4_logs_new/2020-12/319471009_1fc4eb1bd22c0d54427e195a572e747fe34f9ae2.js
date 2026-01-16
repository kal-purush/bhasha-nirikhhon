//load json
const dataPromise = d3.json("samples.json");

function init(){
    dataPromise.then(function(data){
        console.log(data);
        var names = data.names;
        console.log(names);
        d3.select("select").selectAll("option")
        .data(names)
        .enter()
        .append("option")
        .attr("value", function(d){
            return d;
        })
        .html(function(d){
            return d;
        });
        //initialize by default using the first ID in names
        var defaultID = names[0];
        console.log(defaultID);
        //update metadata and the plots
        loadMetadata(defaultID);
        buildPlot(defaultID);
    });
};
//update the metadata
function loadMetadata(ID){
    console.log(ID);
    dataPromise.then(function(data){
        console.log(data);
        var metadataFiltered = data.metadata.filter(md => md.id === parseInt(ID));
        var metadata = metadataFiltered[0];
        console.log(metadata);
        //string holding info of ID
        var metadataString = "";
        for (key in metadata){
            metadataString = metadataString.concat(`<strong>${key}:</strong> ${metadata[key]}<br />\n`);
            console.log(`${key}: ${metadata[key]}`);
        };
        console.log(metadataString);
        //update contents
        d3.select("#sample-metadata").html(metadataString);
    });
};
///build/rebuild the plots
function buildPlot(ID){
    console.log(ID);
    dataPromise.then(function(data){
        console.log(data);
        var sample = data.samples.filter(sample => sample.id === ID);
        console.log(sample);

        var sample_values = sample[0].sample_values;
        console.log(sample_values);

        var otu_ids = sample[0].otu_ids;
        console.log(otu_ids);

        var otu_labels = sample[0].otu_labels;
        console.log(otu_labels);

        var topTenValues = {
            "sample_values":[],
            "otu_ids":[],
            "otu_labels":[]
        }

        for(var i = 0; i < 10; i++){
            topTenValues.sample_values.push(sample_values[i]);
            topTenValues.otu_ids.push(otu_ids[i]);
            topTenValues.otu_labels.push(otu_labels[i]);
        };
        console.log(topTenValues);
        //for bar chart
        var trace1 = {
            x: topTenValues.sample_values,
            y: topTenValues.otu_ids,
            text: topTenValues.otu_labels,
            type: "bar",
            orientation: "h"
        };
        var bar_data=[trace1];
        var bar_layout = {
            title: `Top 10 OTUs found in ${ID}`,
            yaxis: {
                autorange:"reversed"
            }
        };
        Plotly.newPlot("bar", bar_data, bar_layout);
        //for bubble chart
        var trace2 = {
            x: otu_ids,
            y: sample_values,
            text: otu_labels,
            mode: "markers",
            type: "scatter",
            marker:{
                size: sample_values,
                color: otu_ids,
            }
        };
        var bubble_data=[trace2];
        var bubble_layout = {
            title: `Bubble Plot`
        };
        Plotly.newPlot("bubble", bubble_data, bubble_layout);
    });
};
function optionChanged(ID){
    //when the option is changed, we need to load new data and display new plots
    loadMetadata(ID);
    buildPlot(ID);
};

init();