(function () {
     var spareCircles = [30,70,110];
     var svgContainer = d3.select(".main-container").append("svg").attr("height",400).attr("width",400).style("border","1px solid black");
     var circleSelection  = svgContainer.selectAll("circle")
                                        .data(spareCircles)
                                        .enter()
                                        .append("circle")
                                        .attr("cx",function(d){
                                            return d;
                                        })
                                        .attr("cy",function(d){
                                            return d;
                                        })
                                        .attr("r",15)
                                        .style("fill","purple");

    

} ());