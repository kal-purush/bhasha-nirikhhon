// code compiled from 
// http://bl.ocks.org/syntagmatic/4020926
// http://bl.ocks.org/jasondavies/1341281
// http://bl.ocks.org/mbostock/1341021
// http://bl.ocks.org/mostaphaRoudsari/b4e090bb50146d88aec4
// http://bl.ocks.org/mbostock/1087001

ParallelCoordVis_ = function(_parentElement, _session, _eventHandler) {
    var self = this;
    
    self.parallelCoordKeys = [
        // 'Rated',
        // 'Runtime', // minutes
        // 'BoxOffice',
        // 'Director',
        // 'Title',
        // 'Year',
        // 'imdbRating',
        // 'tomatoRating',
        // 'tomatoUserRating'

        'Title',
        'Released',
        'Year',
        'imdbRating',
        'tomatoRating',
        'tomatoUserRating',
        'imdbVotes',
        'tomatoUserRating',
        'BoxOffice'
    ]

    self.parentElement = _parentElement;

    self.eventHandler = _eventHandler;

    self.data = filterData(
        _session.get('actorMovies'), 
        self.parallelCoordKeys
    );

    self.displayData = [];
    self.initVis();
};


ParallelCoordVis_.prototype.initVis = function () {

    var self = this; 

    // find the longest text size in the first row to adjust left margin
    // need to make this better ...
    var textLength = 0;
    self.data.forEach(function(d){
        if (d.Title.length > textLength) {
            textLength = d.Title.length;
        }
    });

    var margin = {top: 30, right: 30, bottom: 10, left: 150 + textLength},
        width = 960 - margin.left - margin.right,
        height = 500 - margin.top - margin.bottom;

    self.dimensions = self.setDimensions(height);

    // var x = d3.scale.ordinal().rangePoints([0, width], 1);
    self.x = d3.scale.ordinal()
    self.y = {};

    
    self.dragging = {};

    self.line = d3.svg.line();
        // .defined(function(d) { return !isNaN(d[1]); });
    self.axis = d3.svg.axis().orient("left");
    self.background;
    self.foreground;
    self.selection;

    self.svg = self.parentElement.append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
      .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    self.div = self.svg.append("div")
        .attr("class", "tooltip-pc")
        .style("display", "none");

    // area is set up, now lets make the vis from data

    self.x.domain(dimen = self.dimensions.map(function(d) {
        if (d.type !== 'string') {
            self.y[d.name] = d.scale.domain(d.extent).range(d.range);
        } else {
            self.y[d.name] = d.scale.rangePoints([0, height]);
        }
        return d.name;
    }))
    .rangePoints([0, width]);    

    self.dimensions.forEach(function(dimension) {
        dimension.scale.domain(dimension.type === "number"
        ? dimension.extent
        : self.data.map(function(d) { 
            return d[dimension.name]; 
        }).sort());
    });

    // Add grey background lines for context.
    self.background = self.svg.append("g")
        .attr("class", "background")
        .selectAll("path")
        .data(self.data)
        .enter().append("path")
        .attr("d", function(d) { return self.path(d) });

    // Add blue foreground lines for focus.
    self.foreground = self.svg.append("g")
        .attr("class", "foreground")
        .selectAll("path")
        .data(self.data)
        .enter().append("path")
        .attr("d", function(d) { return self.path(d) })
        .on("mouseover", function() { self.mouseover() })
        .on("mouseout", function() { self.mouseout() })
        .on("mousemove", function(d) { return self.mousemove(d) });

    self.selection = self.svg.append("g")
        .attr("class", "selection")

    // Add a group element for each dimension.
    self.g = self.svg.selectAll(".dimension")
        .data(dimen)
        .enter().append("g")
        .attr("class", "dimension")
        .attr("transform", function(d) {
            return "translate(" + self.x(d) + ")"; 
        })
        .call(d3.behavior.drag()
            .origin(function(d) { return {x: self.x(d)}; })
            .on("dragstart", function(d) {
                self.dragging[d] = self.x(d);
                self.background.attr("visibility", "hidden");
            })
            .on("drag", function(d) {
                self.dragging[d] = Math.min(width, Math.max(0, d3.event.x));
                self.foreground.attr("d", function(d) { return self.path(d) });
                
                dimen.sort(function(a, b) {
                    return self.position(a) - self.position(b); 
                });                
                self.x.domain(dimen);
                self.g.attr("transform", function(d) { 
                    return "translate(" + self.position(d) + ")"; 
                })
            })
            .on("dragend", function(d) {
                delete self.dragging[d];
                
                self.transition(d3.select(this)).attr(
                    "transform", "translate(" + self.x(d) + ")"
                );
                
                self.transition(self.foreground)
                    .attr("d", function(d) { return self.path(d) });

                self.background
                    .attr("d", function(d) { return self.path(d) })
                    .transition()
                    .delay(500)
                    .duration(0)
                    .attr("visibility", null);
                
                self.selection
                    .attr("d", function(d) { return self.path(d) })
                    .transition()
                    .delay(500)
                    .duration(0)
                    .attr("visibility", null);
            })
        );

    // Add an axis and title.
    self.g.append("g")
        .attr("class", function(d) {
            if (d === 'Title') {
                return 'axis-string';
            }
            return "axis";
        })
        .each(function(d) {
            // d3.select(this).call(axis.scale(y[d]));
            var cdimen = self.checkDimension(d)
            // format the ticks to not have commas
            if (cdimen.type === 'string') {
                d3.select(this).call(self.axis.scale(
                    cdimen.scale
                    ).tickSize(4)
                );
            } else {
                d3.select(this).call(self.axis.scale(
                    cdimen.scale
                    ).tickFormat(d3.format('d'))
                );  
            }
        })
        .append("text")
        .style("text-anchor", "middle")
        .attr("id", "pc-axis-label")
        .attr("y", -9)
        .text(function(d) { return d; });

    // Add and store a brush for each axis.
    self.g.append("g")
        .attr("class", "brush")
        .each(function(d) {
            var cdimen = self.checkDimension(d);
            d3.select(this).call(
                cdimen.brush = d3.svg.brush()
                    .y(self.y[d])
                    .on("brushstart", function() { self.brushstart() })
                    .on("brush", function() {self.brush() }));
        })
        .selectAll("rect")
        .attr("x", -8)
        .attr("width", 16);

    // filter, aggregate, modify data
    // self.wrangleData(null);

    // call the update method
    // self.updateVis();

};


ParallelCoordVis_.prototype.transition = function(g) {
    var self = this;

    return g.transition().duration(500);
}

// Returns the path for a given data point.
ParallelCoordVis_.prototype.path = function(d) {
    var self = this;
    return self.line(dimen.map(function(p) { 
        return [self.position(p), self.y[p](d[p])]; 
    }));
}

ParallelCoordVis_.prototype.position = function(d) {
    var self = this;
    var v = self.dragging[d];
    if (! v) {
        return self.x(d); 
    }
    return v;
    // return v == null ? self.x(d) : v;
}

ParallelCoordVis_.prototype.brushstart = function() {
    var self = this;

    d3.event.sourceEvent.stopPropagation();
}

ParallelCoordVis_.prototype.brush = function() {
    var self = this;

    var actives = self.dimensions.filter(function(p) { 
        return !p.brush.empty(); 
    });
    var extents = actives.map(function(p) { 
        return p.brush.extent(); 
    });

    var selectedData = [];

    self.foreground.style("display", function(d) {
        return actives.every(function(p, i) {
            if (p.type === 'string') {
                // console.log(d, p, self.y[p.name](d[p.name]))
                var r = (extents[i][0] <= self.y[p.name](d[p.name]) && 
                    self.y[p.name](d[p.name]) <= extents[i][1])
                if (r) { selectedData.push(d) }
                return r; 
            } else {
                var r = (extents[i][0] <= d[p.name] && 
                    d[p.name] <= extents[i][1])
                if (r) { selectedData.push(d) }
                return r;
            }
        }) ? null : "none";
    });

    self.eventHandler.selectionChanged(selectedData)
}

ParallelCoordVis_.prototype.checkDimension = function(d) {
    var self = this;

    var cdimen; 
    self.dimensions.forEach(function(dim) {
        if (dim.name === d) {
            cdimen = dim;
        }
    })
    return cdimen
}

ParallelCoordVis_.prototype.mousemove = function(d) {
    var self = this;

    var movie = dimen.map(function(p) { return [d[p]][0]; })

    self.div.html(
        "<div id=tooltipID><strong>Title: </strong>" + movie[0].toString() + "</div>")
        .style("left", function() {
            var xPos = d3.event.pageX;
            if (xPos > 960/2) {
                return (d3.event.pageX) - 305 + "px";
            }
            return (d3.event.pageX - 30) + "px";
        })     
        .style("top", function() {
            var yPos = d3.event.pageY;
            if (yPos > 500 - 10) {
               return (d3.event.pageY - 300) + "px" 

            }
            return (d3.event.pageY - 100) + "px"
        });
    // console.log(d3.event.pageX, d3.event.pageY)
//     self.div.text(movie[0])
//         .style("left", (d3.event.pageX - 34) + "px")
//         .style("top", (d3.event.pageY - 12) + "px");
}


ParallelCoordVis_.prototype.mouseover = function() {
    var self = this;

    var evt = d3.event.target
    
    self.div.style("display", "inline");

    var line = d3.select(evt);
    line.moveToFront();
    line
        .style("stroke", 'black')
        .style("stroke-width", 4)
}

ParallelCoordVis_.prototype.mouseout = function() {
    var self = this;

    var evt = d3.event.target

    self.div.style("display", "none");
    
    line = d3.select(evt);
    if (line.attr('class') === 'selected_line'){
        line
            .style('stroke', 'red')
            .style("stroke-width", 2)
    } else {
        line
            .style('stroke', 'steelblue')
            .style("stroke-width", 2)
    }
}


ParallelCoordVis_.prototype.setDimensions = function(height) {

    var self = this;

    var dimensions = [
        {
            name: "Title",
            scale: d3.scale.ordinal(),
            type: "string",

        },
        // {
        //     name: "Director",
        //     scale: d3.scale.ordinal(),
        //     type: "string",

        // },
        {
            name: "imdbRating",
            scale: d3.scale.linear(),
            extent: [0,10],
            type: "number",
            range : [height, 0],
        },
        {
            name: "tomatoRating",
            scale: d3.scale.linear(),
            extent: [0,10],
            type: "number",
            range : [height, 0],
        },
        {
            name: "tomatoUserRating",
            scale: d3.scale.linear(),
            extent: [0, 5],
            type: "number",
            range : [height, 0],
        },
        {
            name: "BoxOffice",
            scale: d3.scale.log(),
            extent: [0.01, d3.max(self.data, function(p) {
                return +p['BoxOffice'];
            })],
            type: "number",
            range : [height, 0]
        },
        {
            name: "Year",
            scale: d3.scale.linear(),
            extent: d3.extent(self.data, function(p) {
                        return +p['Year'];
                    }),
            type: "number",
            range : [height, 0]
        },
    ];

    return dimensions;
}


ParallelCoordVis_.prototype.updateVis = function (d) {
    var self = this;

    if (!Array.isArray(d)) {
        d = [d]
    }

    // Add blue foreground lines for focus.
    self.selection = d3.selectAll('.selection')
        .attr("class", "selection")
        .selectAll("path")
        .data(d)
        
    self.selection.enter().append("path")

    self.selection
        .attr('class', 'selected_line')
        .attr("d", function(d) { return self.path(d) })
        .on("mouseover", function() { self.mouseover() })
        .on("mouseout", function() { self.mouseout() })
        .on("mousemove", function(d) { return self.mousemove(d) })
        .moveToFront();

    self.selection.exit().remove()

};









// https://gist.github.com/trtg/3922684

d3.selection.prototype.moveToFront = function() {
    return this.each(function(){
        this.parentNode.appendChild(this);
    });
};