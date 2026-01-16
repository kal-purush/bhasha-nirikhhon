import debounce from '../lib/debounce'
import groupArray from 'group-array'

import {
    line as d3_line,
    curveStep as d3_curveStep
    //curveBasis as d3_curveBasis
} from 'd3-shape';

import {
    select as d3_select,
    selectAll as d3_selectAll
} from 'd3-selection';

import { transition } from 'd3-transition';

import {
    min as d3_min,
    max as d3_max,
    sum as d3_sum,
    extent as d3_extent,
    range as d3_range,
    bisector as d3_Bisector
} from 'd3-array'

import {
    scalePoint,
    scaleLinear,
    scaleTime
} from 'd3-scale';

import {
    axisLeft as d3_axisLeft,
    axisRight as d3_axisRight,
    axisBottom as d3_axisBottom
} from 'd3-axis';

import {
    timeFormat as d3_timeParse
} from 'd3-time-format';

var parseTime = d3_timeParse("%Y-%m-%d"); //("%d-%b-%y"); //;

let dataset, localData;
let svgshell, svg, lines;


export default function() {

    var xhr = new XMLHttpRequest();
    xhr.onreadystatechange = function() {
        if (xhr.readyState == XMLHttpRequest.DONE) {
            dataset = JSON.parse(xhr.responseText);

           // for (var i = 0; i < dataset.attendances.length; i++) {
           //      dataset.attendances[i].d3Date = parseTime(new Date(dataset.attendances[i].date));
           //      //dataset.attendances[i].homeTeam == "Arsenal" ? arsenal.push(dataset.attendances[i]) : spurs.push(dataset.attendances[i]) ;
           //  }

            //localData = dataset.attendances;

            //console.log(dataset);

           initViz(dataset)
			    
		


            //buildVisual(localData);

        }
    }

    xhr.open('GET', '<%= path %>/assets/data/scorers.json', true);
    xhr.send(null);


    function initViz(data){

    	let seasonContainers = document.querySelectorAll(".season-container");

    	var margin = {top: 20, right: 20, bottom: 30, left: 30}, width = 145, height = 120;

    	data.forEach(function(dataEl){
    		var targetContainers = [];
    		let a = dataEl.scorerRef;
	    		seasonContainers.forEach(function(el){
	    			let b = el.getAttribute("player");
	    			let c = el.getAttribute("season");

	    			if(a==b){ targetContainers.push(el) }
	    		})
	    	buildCharts(a,targetContainers,dataEl)
    	})

		console.log(data,seasonContainers);

    }


    function buildCharts(player,containers,data){
    	let a = data.seasonsPlayed;

    	a.forEach(function(season){  
    		containers.forEach(function(el){
	    		if(el.getAttribute("player")==player && el.getAttribute("season")==season){
	    			addChart(el,data,season)
	    		}
	    	})
    	})
    }


    function addChart(el,data,season){

    	let matchData = [];

    	data.matches.forEach(function(match){
    		if(match.season==season){
    			matchData.push(match)
    		}
    	})

    	var margin = {top: 20, right: 20, bottom: 30, left: 30},
		    width = 140,
		    height = 120;

    	var minDate = new Date(season.split("-")[0],7,1);
    	var maxDate = new Date(season.split("-")[1],6,1);

    	var container = d3_select(el);

    	var x = scaleLinear().range([0, width]); 
    	var y = scaleTime().range([height, 0]);  
		x.domain([0, 10]); 
    	y.domain([minDate, maxDate]);

    	var matchline = d3_line()
		    .x(function(d) { return x(d.date); })
		    .y(function(d) { return y(d.close); });
		
    	var svg = container.append("svg")
		    .attr("width", width)
		    .attr("height", height);

		  //Add the Y Axis
		  svg.append("g")
		  	.append("line")
            .attr("x1", (width/2)-0.5)
            .attr("y1", 0)
            .attr("x2",  (width/2)-0.5)
            .attr("y2", height);

          svg.append("text")
              .attr("x", 0)
              .attr("y", 10)
              .text( function () { return season })
              .attr("class", "season-label");  

        matchData.forEach(function(match){
        	match.date = new Date(match.Date);

        	//console.log(match)

        	let yPos = y(match.date)

        	let goalW = 10;
        	
        	let matchG = svg.append("g")
			  	.append("line")
			  	.attr("class","matchline")
	            .attr("x1", 0)
	            .attr("y1", yPos)
	            .attr("x2",  width)
	            .attr("y2", yPos);

			// addGoals
	        for(var i = 0; i < match.AwayGoals.length; i++){
	        	var newX = Math.ceil(width/2);
	        	var rectangle = svg.append("g")
	        		.append("rect")
                    .attr("x", width/2 + (i*11))
                    .attr("y", yPos - 5)
                    .attr("width", 10)
                    .attr("height", 10)
                    .attr("fill","#CCC");
	        }  

	        // addGoals
	        for(var i = 0; i < match.HomeGoals.length; i++){
	        	var newX = Math.floor(width/2);
	        	var rectangle = svg.append("g")
	        		.append("rect")
                    .attr("x", width/2 - (i*11))
                    .attr("y", yPos - 5)
                    .attr("width", 10)
                    .attr("height", 10)
                    .attr("fill","#CCC")
                    
	        }         

        })

    }

    function buildVisual(data) {    
    	var container = d3_select(".attendance-chart");

    	console.log(container.node().getBoundingClientRect().width)

    	var margin = {top: 20, right: 20, bottom: 30, left: 30},
		    width = container.node().getBoundingClientRect().width,
		    height = container.node().getBoundingClientRect().height;

		// parse the date / time
		//var parseTime = d3_timeParse("%d-%b-%y");

		var mindate = new Date(1991,7,1),
            maxdate = new Date(2018,0,31);

        var x = scaleTime().range([0, width]);   
        // map these the the chart width = total width    
		// set the ranges
		//var x = scaleTime().range([0, width - margin.left])
		var y = scaleLinear().range([height, 0]);

		// define the line
		var valueline = d3_line()
		    .x(function(d) { return x(d.date); })
		    .y(function(d) { return y(d.close); });

		// append the svg obgect to the body of the page
		// appends a 'group' element to 'svg'
		// moves the 'group' element to the top left margin
		var svg = container.append("svg")
		    .attr("width", width + margin.left + margin.right)
		    .attr("height", height + margin.top + margin.bottom)
		  	.append("g")
		    .attr("transform",  "translate(" + margin.left + "," + margin.top + ")");

		// Get the data

		  // format the data
		  data.forEach(function(d) {
		      d.date = d.d3Date;
		      d.close = +d.attendanceNum;
		  });

		  // Scale the range of the data

		  x.domain([mindate, maxdate]) 
		 // x.domain([0,d3_max(data, function(d) { return d.date; })]);
		  y.domain([20000, d3_max(data, function(d) { return d.close; }) +5000]);

		  // Add the valueline path.


		  // Add the X Axis
		  svg.append("g")
		  	.attr('class', 'customXaxis')	
		    .attr("transform", "translate(0," + height + ")")
		    .call(d3_axisBottom(x));

		  // Add the Y Axis
		  svg.append("g")
		  	.attr('class', 'customYaxis')
		    .call(d3_axisRight(y));




		  svg.append("path")
		      .data([data])
		      .attr("class", "line")

		      //.attr("d", valueline);
			
			let lineTypes = ['a', 't'];

		  	lineTypes.forEach(function(l){
    		let typeGroup = svg.append("g")

    		typeGroup.append('path')
    			.attr("class", d => `riskLine riskLineBase ${l}`)
    			.attr("id", l)

    		typeGroup.append('path')
    			.attr("class", d => `riskLine riskLineUser ${l}`)
    			.attr("id", l)
    			
    		});
    		

		  d3_selectAll("g.customYaxis g.tick line")
    		.attr("x1", 0-margin.left)
    		.attr("x2", width-margin.left)

	    	d3_selectAll("g.customYaxis g.tick text")
	    		.attr("x", 0-margin.left)
	    		.attr("y", -9)

	    		//.attr("x2", width-margin.left)
	    		//.attr("stroke-dasharray", "1, 3");

	    	d3_selectAll(".customYaxis .tick text")
			    .filter(function (d) { console.log(d); if (d==20000){ this.remove()}; if (d!=20000){ this.innerHTML = this.innerHTML.split(",")[0];} })
			    			
		
		drawVisual(data,x,y);	

		customYAxis()

    }

 	window.addEventListener('resize', function(){
 		resizeChart()
 	});

	var resizeChart = debounce(function() {
		//drawVisual();
	}, 250);




	function drawVisual(data,xscale,yscale){

		let tempArr = groupArray(data, 'homeTeam');

		d3_selectAll('.riskLine')
				.attr('d', function(data){
					let type = d3_select(this).attr('id');
					var curveData;

					type=='a' ? curveData = tempArr.Arsenal :  curveData = tempArr.Spurs;

					if (type=='a'){
						let valueline = d3_line()
						.curve(d3_curveStep)
						//.interpolate("basis")
					    .x(function(d) { return xscale(new Date(d.d3Date)); })
					    .y(function(d) { return yscale(d.attendanceNum); })

					    return valueline(curveData);
					}

					if (type=='t'){
						let valuelineT = d3_line()
						.curve(d3_curveStep)
						//.interpolate("basis")
					    .x(function(d) { return xscale(new Date(d.d3Date)); })
					    .y(function(d) { return yscale(d.attendanceNum); })

					    return valuelineT(curveData);
					}
					
					

					
			})

				//animate

		// if(window.navigator.userAgent.toLowerCase().indexOf('firefox') <  0){
  //         d3_selectAll('.riskLine').selectAll("path.line").each(function(d,i) {
  //           var eachPath = d3_select(this),
  //           totalLength = eachPath.node().getTotalLength();
  //           eachPath.attr("stroke-dasharray", totalLength + " " + totalLength)
  //           .attr("stroke-dashoffset", totalLength)
  //           .transition()
  //           .duration(1800)
  //           .ease("linear")
  //           .attr("stroke-dashoffset", 0);
  //         });
  //       }


	}



	function customXAxis(g) {
	  //g.call(xaxis).tickValues(d3_range(0, 80000, 10000));
	  g.select(".domain").remove();
	}

	function customYAxis() {
		console.log( d3_select(".customYaxis"))

	  d3_select(".customYaxis")
	  .select(".domain")
	  // .remove().selectAll(".tick:not(:first-of-type) line").attr("stroke", "#777").attr("stroke-dasharray", "2,2")
	  .selectAll(".tick text").attr("x", 4).attr("dy", -12);
	}

	// Returns an attrTween for translating along the specified path element.
	function translateAlong(path) {

	  var l = path.getTotalLength();

	  return function(d, i, a) {
	    return function(t) {
	      var p = path.getPointAtLength(t * l);
	      return "translate(" + p.x + "," + p.y + ")";
	    };
	  };
	}

	function measure(){
		let box = container.node().getBoundingClientRect();
		let WIDTH = box.width;
    	let HEIGHT = box.width * .5;//box.height;

			if(HEIGHT < 300){
				 HEIGHT = 300;
			}

		return {
			WIDTH: WIDTH,
			HEIGHT: HEIGHT
		}
	}

	return {
		init: buildVisual

	}

}