function drawGridMap(url) {
  
  let regions = [];

  let color = d3.scaleThreshold()
        .range(['#99372e', '#a95a4f', '#b77b70', '#c49c92', '#d1bcb4', '#e0dcd4'].reverse())
        .domain([1000, 5000, 10000, 20000, 30000, 51000]);

  let legendLinear = d3.scaleLinear()
                .domain(color.domain())
                .range(color.range());
  
  let legend = d3.legendColor()
                .cells([1000, 5000, 10000, 20000, 30000, 51000])
                .orient('horizontal')
                .scale(legendLinear)
                .labelFormat(d3.format(",.0f"))
                .title('');
        

  let width, height, cellSize, regionGroup, text, numb, title, credit1, credit2;

  let svg = d3.select('#svgGrid');
  let region = svg.append("g");
  
  svg.append('g')
      .attr('class', 'legend')



    //let buttons = [{1: 'Плиточная'}, {2: 'Geo'}];

  d3.select("#grid").text().split("\n").forEach(function(line, i) {
    
    let re = /[\wа-я]+/ig;
    let array1;

    while (array1 = re.exec(line)) regions.push({
      name: array1[0],
      x: array1.index / 5,
      y: i
    })
  });

  d3.json(url, d =>{
    let jsonRegions = d.objects.rus_regions_simpl.geometries;
    for (let i = 0; i < jsonRegions.length; i++){
      let jsonName = jsonRegions[i].properties.short;
      console.table(jsonRegions[i].properties.short)
      for (let j = 0; j < regions.length; j++){
        let arrName = regions[j].name;
        if(jsonName == arrName){
          regions[j]["migrants"] = parseInt(jsonRegions[i].properties.migrants);
          regions[j]["tariff"] = parseInt(jsonRegions[i].properties.tariff);
          regions[j]["nelegal"] = jsonRegions[i].properties.nelegal;
          console.log(regions[j]["nelegal"]);
          console.log(regions[j]["migrants"]);
        }
      }
    }
    draw()
  });

  let gridWidth = d3.max(regions, function(d) { return d.x; }) + 1,
      gridHeight = d3.max(regions, function(d) { return d.y; }) - 0.01;


  function draw(){

      var mouseover = function(d) {
        tooltip
          .style("opacity", 1)
      }
    regionGroup = region.selectAll(null)
          .data(regions)
          .enter()
          .append('g')
          .attr("class", function(d){ return 'region ' + d.name})
          .append('rect')
          .attr('fill', function(d){
            return color(d.migrants)
          })
       .on("mousemove", function(d) {
          d3.select(this).style('opacity', 0.8)})
         .on('mouseout', function(d) {
          d3.select(this).style('opacity', 1)});

    regionGroup.attr("transform", function(d) { 
                        return "translate(" + (d.x - gridWidth / 2) * cellSize + "," + (d.y - gridHeight / 2) * cellSize + ")"; })
                    .attr("x", -cellSize / 2)
                    .attr("y", -cellSize / 2)
                    .attr("width", cellSize - 1)
                    .attr("height", cellSize - 1);
    text = region.selectAll('g')
                    .append('text')
                    .attr('class', 'regionName')
                    .attr('text-anchor', 'middle')
                    .text(function(d){if (d.name==='ЛЕНИ'){
                      return (d.name).replace('И','') + '+СПБ'
                    }
                    else {return d.name;}

                    });

    function numberWithSpaces(x) {
      return x.toString().replace(/\B(?=(\d{3})+(?!\d))/g, " ");
}
    numb = region.selectAll('g')
          .append('text')
          .attr('class', 'migrantsNum')
          .attr('text-anchor', 'middle')
          .text(function(d){
            return numberWithSpaces(d.migrants);
          });

    title = svg.append("text")
        .attr('class', 'title')
        .attr("x", 10)
        .attr("y", 25 )
        .style("fill", "#676767")
        .text("В какие регионы приезжали мигранты в 2018 году (в абсолютных значениях)");

    credit1 = svg.append("text")
        .attr('class', 'credits')
        .attr("x", 10)
        .attr("y", 550)
        .style("fill", "#676767")
        .text("Важные истории, 2020");

    credit2 = svg.append("text")
        .attr('class', 'credits')
        //.attr("x", 10)
        //.attr("y", 570 )
        .style("fill", "#676767")
        .text("Данные: Росстат");


    d3.select(window).on('resize', resize);
    resize()
  };


  function resize(){
    
    let bounds = d3.select('#viz').node().getBoundingClientRect();
    width = bounds.width;
    height = width/1.85;
    cellSize = width * 0.045;

    svg
      .attr('width', width)
      .attr('height', height)

    title
      .attr('x', width/95)
      .attr('y', height*0.05)

    credit2
      .attr('x', width/95)
      .attr('y', height*0.99)

    credit1
      .attr('x', width/95)
      .attr('y', height*0.95)


    region.attr("transform", `translate(${width/2}, ${height/2})`);

    regionGroup.attr("transform", function(d) { 
            return "translate(" + (d.x - gridWidth / 2) * cellSize + "," + (d.y - gridHeight / 2) * cellSize + ")"; })
        .attr("x", -cellSize / 2)
        .attr("y", -cellSize / 2)
        .attr("width", cellSize - 1)
        .attr("height", cellSize - 1);

    text
        .attr("transform", function(d) { 
            return "translate(" + (d.x - gridWidth / 2) * cellSize + "," + (d.y - (gridHeight / 2)-0.1) * cellSize + ")"; })

      numb.attr("transform", function(d) {
            return "translate(" + (d.x - (gridWidth / 2)) * cellSize + "," + (d.y - (gridHeight / 2)+0.25) * cellSize + ")"; })

    
    legend.shapeWidth(cellSize);
    svg.select(".legend")
            .attr("transform", `translate(${width - cellSize*10},${height - cellSize*2.5})`)
            .call(legend);
  };

  function mouseover (d, element) {
    console.table(d.name, d.nelegal)
  };
};


drawGridMap('data.json');







