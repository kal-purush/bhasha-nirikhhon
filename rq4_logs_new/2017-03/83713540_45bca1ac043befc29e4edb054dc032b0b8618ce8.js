var graph = document.getElementById("graph");
var graphUpdateTime = 2000;
var d = new Date();
var time = d.toTimeString();
var xPoints = [time];
var yPoints = [0];
var minRangeX = 0;
var maxRangeX = 10;
var minRangeY = 0;
var maxRangeY = 10;





var tweetTrace = {
	x: [time],
	y: [tweetCountCurrent],
	fill: "tonexty",
	type: "scatter"
}

var data = [tweetTrace];

var layout = {
    title: "Static Tweet Chart",
    paper_bgcolor: 'rgba(0,0,0,0)',
  	plot_bgcolor: 'rgba(0,0,0,0)',
  	showlegend: false,
  	// fixedrange: disable zooming and panning
  	// range: set ranges to initialized values
  	xaxis: 	{
  		fixedrange: true,
  		range: [minRangeX, maxRangeX]
  	},
  	yaxis: 	{
  		fixedrange: true,
  		range: [minRangeY, maxRangeY]
  	}
};

// initial plot on page load
Plotly.newPlot(graph, data, layout, {displayModeBar: false});


setInterval(function(){
	// grab current date, convert to string, and push to xPoint array
	d = new Date();
	time = d.toTimeString();
	xPoints.push(time);

	// push current tweet count (updated in socket section of main.js) to yPoint array
	yPoints.push(tweetCountCurrent);

	// check if 5 points have been added. If so, update x axis range
	if (xPoints.length%5 === 0) {
		minRangeX = xPoints.length - 5;
		maxRangeX = xPoints.length + 5;
	}
	// updated y axis range to 5 points higher than the max tweetcount
	maxRangeY = Math.max(...yPoints) + 5;

	// animate new points being added to graph
	Plotly.animate(graph, {
	    data: [{x: xPoints, y: yPoints}],
	    traces: [0],
	    layout: {
	    	// update ranges with new mins and maxes
	    	xaxis: 	{
  				range: [minRangeX, maxRangeX]
  			},
	    	yaxis: 	{
  				range: [minRangeY, maxRangeY]
  			}
	    }
	  }, {
	    transition: {
	      duration: 500,
	      easing: 'cubic-in-out'
	    }
	  })

	// reset tweet count (we're measuring tweets per X seconds)
	tweetCountCurrent = 0;
}, graphUpdateTime);

