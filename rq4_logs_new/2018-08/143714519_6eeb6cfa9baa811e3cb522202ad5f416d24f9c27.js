


//acquiring file system
var fs = require('fs');

//acquiring readline
var readline = require('readline');


//acquiring wink-statistics module
var bxplt = require('wink-statistics').stats.boxplot;
var freqTable = require('wink-statistics').streaming.freqTable;
var ft = freqTable();

//acquiring plotly
var username = 'username';
var api_key = 'api_key';
var plotly = require('plotly')(username, api_key);


//Reading the CSV file
var instream = fs.createReadStream('flower.csv');

//variable declaration 
var sepal_length =[];
var sepal_width =[];
var petal_length =[];
var petal_width =[];
var species = [];
var index = 0;
var details = [];
var data =[];
var layout ={};
var trace0 ={};
var trace1 ={};
var trace2 ={};
var trace3 ={};

//Creating interferance for input and output
var rl = readline.createInterface(
{
    input: instream
});

 function data_setup(sep_len,sep_wid,pet_len,pet_wid,spe_c,plot_type)
 {

    

       trace0 = 
    {
     opacity:0.75,
      name:"sepal_length"
    };

   trace1 = 
    {
      opacity:0.75,
      name:"sepal_width"
    };

   trace2 = 
    {
      opacity:0.75,
      name:"petal_length"
    };

   trace3 = 
    {
      opacity:0.75,
      name:"petal_width",
    };


   
  if(plot_type=='histogram')
  {
      trace0 = Object.assign(trace0,{x: sep_len,type:"histogram"});
      trace1 = Object.assign(trace1,{x:sep_wid,type:"histogram"});
      trace2 = Object.assign(trace2,{x:pet_len,type:"histogram"});
      trace3 = Object.assign(trace3,{x:pet_wid,type:"histogram"});
      data = [trace0, trace1,trace2,trace3];
  }

  else if(plot_type=='boxplot')
  {   
      
      trace0 = Object.assign(trace0,{y: sep_len,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
      trace1 = Object.assign(trace1,{y: sep_wid,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
      trace2 = Object.assign(trace2,{y: pet_len,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
      trace3 = Object.assign(trace3,{y: pet_wid,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
      data = [trace0, trace1,trace2,trace3];
  }

  else if (plot_type=="group_boxplot")
  {
    trace0 = Object.assign(trace0,{x:species,y:sep_len,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
    trace1 = Object.assign(trace1,{x:species,y:sep_wid,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
    trace2 = Object.assign(trace2,{x:species,y:pet_len,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
    trace3 = Object.assign(trace3,{x:species,y:pet_wid,boxpoints: "all",jitter: 0.3,pointpos: -1.8,type: "box"});
    data = [trace0, trace1,trace2,trace3];

  }

  
}



function plot(type)
{ 
  layout= {
            title:` ${type}  for IRIS flower`,
            xaxis:{title:"range"},
            yaxis:{title:"frequency"},
          }; 

  if(type=='histogram')
  {
   layout = Object.assign(layout,{bargap:0.25,barmode:"overlay"});
   var graphOptions = {layout: layout, filename: "overlaid-histogram",fileopt: "overwrite"};
   plotly.plot(data, graphOptions, function (err, msg) {
    console.log(msg);
  });
  }

  else if(type=='boxplot')
  {
    var graphOptions = {layout: layout, filename: "box-plot-jitter",fileopt: "overwrite"};
    plotly.plot(data, graphOptions, function (err, msg) {
    console.log(msg);
  });
  }

  else if (type =="group_boxplot")
  {
    var graphOptions = {layout: layout, filename: "box-plot-group",fileopt: "overwrite"};
    plotly.plot(data, graphOptions, function (err, msg) 
    {
    console.log(msg);
    });
  }

}



//Reading Streaming line one by one
rl.on('line', function(line)
{
   str = line;
   var arr = str.split(",").map(function (val) { return val; });
   if(index>0)
   {
   
    sepal_length.push(+arr[0]);
    sepal_width.push(+arr[1]);
    petal_length.push(+arr[2]);
    petal_width.push(+arr[3]);
    species.push(arr[4]);
    ft.build(arr[4])
   }
   index++;
});


//After the line is stream ,end computation is performed
rl.on('close',function()
{

  //visualizing through histogram
  data_setup(sepal_length,sepal_width,petal_length,petal_width,species,"histogram");
  plot("histogram");

  //visualizing through boxplot
  data_setup(sepal_length,sepal_width,petal_length,petal_width,species,"boxplot");
  plot("boxplot");

  //visualizing through group_boxplot according to the species
  data_setup(sepal_length,sepal_width,petal_length,petal_width,species,"group_boxplot");
  plot("group_boxplot");

});