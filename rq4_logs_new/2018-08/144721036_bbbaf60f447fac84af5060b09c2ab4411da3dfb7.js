//acquirng dataset
//Global variable initialization
var w = 600;
var h = 600;
var padding = 50;
var dataset;	
var x_scale;
var	y_scale;
var x_axis;
var y_axis;
var text;



function plot(data_set)
{

	//  creating the SVG element
	var svg = d3.select("body")
				.append("svg")
				.attr("width",w)
				.attr("height",h);

		svg.selectAll("circle")
			.data(data_set)
			.enter()
			.append("circle")
			.attr("cx",function(d){
				//console.log("x axis sepal width");
				//console.log(x_scale(d[0]));
				return x_scale(d[0]);
			})
			.attr("cy",function(d){
				//console.log("y axis sepal length");
				//console.log(y_scale(d[1]));
				return y_scale(d[1]);
			})
			.attr("r",5)
			.attr("fill",function(d)
				{
					if(d[2] == "sentosa")
					{
						return "black";
					}

					else if(d[2] == "versicolor")
					{
						return "red";
					}
					else if(d[2] == "virginica")
					{
						return "green";
					}
				});
	
		

    	svg.append("g")
           .attr("transform", "translate(0,550)")
           .call(x_axis);

        svg.append("g")
       	   .attr("transform", "translate(50, 0)")
       	   .call(y_axis);


       // add a title
	svg.append("text")
        .attr("x", w/2)             
        .attr("y",  padding)
        .attr("text-anchor", "middle")  
        .style("font-size", "18px")   
        .text(text);

}




d3.csv("flower.csv").then(function(data)
{
	function data_setup()
	{
		var x_data;
		var y_data;
		var x_max;
		var y_max;
		var x_min;
		var y_min;
	
		var number = +prompt(" Enter the number 1 to 12 for the scatter plot graph according to the number code."+
   		 "\n" + "Press 1 for sepal_width versus sepal_length"+
   		 "\n" + "Press 2 for petal_length versus sepal_length"+
   		 "\n" + "Press 3 for petal_width versus sepal_length"+
   		 "\n" + "Press 4 for sepal_length versus sepal_width"+
   		 "\n" + "Press 5 for petal_length versus sepal_width"+
   		 "\n" + "Press 6 for petal_width versus sepal_width"+
   		 "\n" + "Press 7 for sepal_length versus petal_length"+
   		 "\n" + "Press 8 for sepal_width versus petal_length"+
   		 "\n" + "Press 9 for petal_width versus petal_length"+
   		 "\n" + "Press 10 for sepal_length versus petal_width"+
   		 "\n" + "Press 11 for sepal_width versus petal_width"+
   		 "\n" + "Press 12 for petal_length versus petal_width");
		switch (number) 
   		{
   			case 1:
        	dataset = data.map(function(i)
        	{
        		console.log("case 1 selected");
        		
        		return [+(i.sepal_width),+(i.sepal_length),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.sepal_width);
			});
			y_data =data.map(function(i)
			{
				return +(i.sepal_length);
			});
			text = "sepal_width versus sepal_length";
        	break;

    		case 2:
    		dataset = data.map(function(i)
        	{	
        		console.log("case 2 selected");
        		return [+(i.petal_length),+(i.sepal_length),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.petal_length);
			});
			y_data =data.map(function(i)
			{
				return +(i.sepal_length);
			});
			text = "petal_length versus sepal_length";
			break;

    		case 3:
        	dataset = data.map(function(i)
        	{
        		console.log("case 3 selected");
        		
        		return [+(i.petal_width),+(i.sepal_length),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.petal_width);
			});
			y_data =data.map(function(i)
			{
				return +(i.sepal_length);
			});
			text = "petal_width versus sepal_length";
        	break;

    		case 4:
        	dataset = data.map(function(i)
        	{
        		console.log("case 4 selected");
        		return [+(i.sepal_length),+(i.sepal_width),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.sepal_length);
			});
			y_data =data.map(function(i)
			{
				return +(i.sepal_width);
			});
			text = "sepal_length versus sepal_width";
        	break;

    		case 5:
        	dataset = data.map(function(i)
        	{
        		console.log("case 5 selected");
        		return [+(i.petal_length),+(i.sepal_width),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.petal_length);
			});
			y_data =data.map(function(i)
			{
				return +(i.sepal_width);
			});
			text = "petal_length versus sepal_width";
        	break;

    		case 6:
        	dataset = data.map(function(i)
        	{
        		console.log("case 6 selected");
        		return [+(i.petal_width),+(i.sepal_width),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.petal_width);
			});
			y_data =data.map(function(i)
			{
				return +(i.sepal_width);
			});
			text = "petal_width versus sepal_width";
        	break;

        	case 7:
        	dataset = data.map(function(i)
        	{
        		console.log("case 7 selected");
        		return [+(i.sepal_length),+(i.petal_length),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.sepal_length);
			});
			y_data =data.map(function(i)
			{
				return +(i.petal_length);
			});
			text = "sepal_length versus petal_length";
			break;

			case 8:
        	dataset = data.map(function(i)
        	{
        		console.log("case 8 selected");
        		return [+(i.sepal_width),+(i.petal_length),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.sepal_width);
			});
			y_data =data.map(function(i)
			{
				return +(i.petal_length);
			});
			text = "sepal_width versus petal_length";
			break;

			case 9:
        	dataset = data.map(function(i)
        	{
        		console.log("case 9 selected");
        		return [+(i.petal_width),+(i.petal_length),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.petal_width);
			});
			y_data =data.map(function(i)
			{
				return +(i.petal_length);
			});
			text = "petal_width versus petal_length";
			break;

			case 10:
        	dataset = data.map(function(i)
        	{
        		console.log("case 10 selected");
        		return [+(i.sepal_length),+(i.petal_width),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.sepal_length);
			});
			y_data =data.map(function(i)
			{
				return +(i.petal_width);
			});
			text = "sepal_length versus petal_width";
			break;

			case 11:
        	dataset = data.map(function(i)
        	{
        		console.log("case 11 selected");
        		return [+(i.sepal_width),+(i.petal_width),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.sepal_width);
			});
			y_data =data.map(function(i)
			{
				return +(i.petal_length);
			});
			text = "sepal_width versus petal_width";
			break;

			case 12:
        	dataset = data.map(function(i)
        	{
        		console.log("case 12 selected");
        		return [+(i.petal_length),+(i.petal_width),i.species];
			});
			x_data = data.map(function(i)
        	{
        		
        		return +(i.petal_length);
			});
			y_data =data.map(function(i)
			{
				return +(i.petal_width);
			});
			text = "petal_length versus petal_width";
			break;

			default:
			alert("The number you are dialing is currently  not included on the list");
		}

		x_max = d3.max(x_data);
		y_max = d3.max(y_data);
		x_min =	d3.min(x_data);
		y_min = d3.min(y_data);
		//console.log(x_max);
		//console.log(y_max);

		//creating the scale
		x_scale = d3.scaleLinear()
    					.domain([x_min-1,x_max+1])
    					.range([padding,w-padding]);
   	
   		y_scale = d3.scaleLinear()
   						.domain([y_min-1,y_max+1])
   						.range([h-padding,padding]);

   		// Add scales to axis
    	x_axis = d3.axisBottom()
                   .scale(x_scale);


        y_axis = d3.axisLeft()
        		   .scale(y_scale);
   	}	
	data_setup();
	//console.log(dataset);
	plot(dataset);

	
})
				

		

	