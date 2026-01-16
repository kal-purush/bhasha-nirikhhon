
var width = window.innerWidth
var height =window.innerHeight

var projection = d3.geoMercator()
    // .translate([width / 2, height / 2])
    	.scale((width - 1) / 2 / Math.PI);

var zoom = d3.zoom()
    .scaleExtent([0.8, 80])
    .on("zoom", zoomed);

var path = d3.geoPath()
    .projection(projection);

var svg = d3.select('svg')
			.attr('width',width)
			.attr('height',height)
			.append("g");

var g = svg.append("g");

svg.append("rect")
    .attr("class", "overlay")
    .attr("width", width)
    .attr("height", height);

svg.call(zoom)

function zoomed() {
  g.attr("transform", d3.event.transform);
}
var div = d3.select('body').append("div").attr('class','tooltip')

d3.json("https://raw.githubusercontent.com/d3/d3-geo/master/test/data/world-50m.json", function(error,world) {

	// sea
  if (error) throw error;
  console.log('123')
  g.append("path")
      .datum({type: "Sphere"})
      .attr("class", "sphere")
      .attr("d", path);

  // land
  g.append("path")
      .datum(topojson.merge(world, world.objects.countries.geometries))
      .attr("class", "land")
      .attr("d", path);
  // country border
  g.append("path")
      .datum(topojson.mesh(world, world.objects.countries, function(a, b) { return a !== b; }))
      .attr("class", "boundary")
      .attr("d", path);
  d3.json("https://raw.githubusercontent.com/FreeCodeCamp/ProjectReferenceData/master/meteorite-strike-data.json", function( alldata ) {

	g.append('text').text('Meteorites').attr('font-size',40).attr('y',300)
	.attr('fill','red').attr('opacity',0.7)
	g.selectAll("circle")
		.data(alldata.features).enter()
		.append("circle")
		.attr("cx", function (d) {if (d.geometry)return projection(d.geometry.coordinates)[0]; })
		.attr("cy", function (d) { if (d.geometry) return projection(d.geometry.coordinates)[1]; })
		.attr("r", function(d) { if(d.properties) return Math.pow(d.properties.mass/100,1/3)})
		.attr("fill", 'rgba(250,0,0,0.2)')
		.attr('stroke-width',0.2)
		.attr('stroke','blue')
		.on('mouseover', function (data) {
		        div
		        .html(formatMeteorites(data.properties))
		        .style("top", (event.pageY-10)+"px")
		        .style("left",(event.pageX+10)+"px")
		        .style('opacity',0.7)
		        d3.select(this).attr('fill','rgba(250,0,0,0.4)')
		  })
		  .on('mouseout',function(data) {
		    div.style('opacity',0)
		    d3.select(this)
		    	.attr("fill", 'rgba(250,0,0,0.2)')
		  })
		   .on('mousemove',function(data) {
		     div.style("top", (event.pageY-10)+"px")
		        .style("left",(event.pageX+10)+"px")

		  })

	function formatMeteorites(d){
		var text = "Name: "+d.name+"<br/>"
		text += "fall: " + d.fall+"<br/>"
		text += "recclass: " + d.recclass+"<br/>"
        text += "reclat: " +d.reclat+"<br/>"
        text += "Time: "+d.year.slice(0,10)+"<br/>"
        return text
	}

	})
  
});



d3.select(self.frameElement).style("height", height + "px");
