var margin = { top: 20, left: 75, bottom: 50, right: 50 },
    width = 850 - margin.left - margin.right,
    height = 350 - margin.top - margin.bottom;

var svg = d3.select('#chart').append('svg')
    .attr('width', width + margin.left + margin.right)
    .attr('height', height + margin.top + margin.bottom)
    .append('g')
    .attr('transform', 'translate(' + margin.left + ', ' + margin.top + ')');

var all, top5, bottom5;

d3.csv('co2_per_capita.csv', d => {
    return {
        country: d.country,
        year:d.year,
        co2_per_capita: +d.co2_per_capita
    };
}).then(data => {
    data = data.filter(function(d){ return d.year==2020 })
    data.sort((a, b) => d3.descending(a.co2_per_capita, b.co2_per_capita));
    console.log(data);
    console.log(data.sort((a, b) => d3.descending(a.co2_per_capita, b.co2_per_capita)));

    //initialize data variables
    all = data;
    top5 = data.slice(0, 5);  //set bottom 5 CO2 emission using array.slice here
    bottom5 = data.slice(data.length - 5);  //set bottom 5 CO2 emission using array.slice here

    //set initial state
    filter('#all');
    sort('#alphabetical');

    toggleFilter('#all');
    toggleSort('#alphabetical');

    draw();
});

///////////////////////////////////////////////////////////////
// Controls
///////////////////////////////////////////////////////////////

var current, sortMode, filterMode;

//sort event handlers
d3.select('#alphabetical')
.on('click', () => {
    sort('#alphabetical');
    transition();
    toggleSort('#alphabetical');
});

d3.select('#ascending_co2')
.on('click', () => {
    sort('#ascending_co2');
    transition();
    toggleSort('#ascending_co2');
});

d3.select('#descending_co2')
.on('click', () => {
    sort('#descending_co2');
    transition();
    toggleSort('#descending_co2');
});

//filter event handlers
d3.select('#all')
.on('click', () => {
    filter('#all');
    sort(sortMode);

    toggleSort(sortMode);
    toggleFilter('#all');

    redraw();
});

d3.select('#top5')
.on('click', () => {
    filter('#top5');
    sort(sortMode);

    toggleSort(sortMode);
    toggleFilter('#top5');

    redraw();
});

d3.select('#bottom5')
.on('click', () => {
    filter('#bottom5');
    sort(sortMode);

    toggleSort(sortMode);
    toggleFilter('#bottom5');

    redraw();
});

//Reset event handlers
d3.select('#reset')
.on('click', () => {
    filter('#all');
    sort('#alphabetical');

    toggleFilter('#all');
    toggleSort('#alphabetical');

    redraw();
});


function filter(mode) {
    if (mode === '#all') {
        current = JSON.parse(JSON.stringify(all));
    } else if (mode === '#top5') {
        current = JSON.parse(JSON.stringify(top5));
    } else if (mode === '#bottom5') {
        current = JSON.parse(JSON.stringify(bottom5));
    }
    filterMode = mode;
}

function sort(mode) {
    if (mode === '#alphabetical') {
        current.sort((a, b) => d3.ascending(a.country, b.country));
        d3.select('#xlabeltext').text('Sorted Alphabetically');
    } else if (mode === '#ascending_co2') {
        current.sort((a, b) => d3.ascending(a.co2_per_capita, b.co2_per_capita));
        d3.select('#xlabeltext').text('Sorted from smallest to largest CO2 Emission');
    } else if (mode === '#descending_co2') {
        current.sort((a, b) => d3.descending(a.co2_per_capita, b.co2_per_capita));
        d3.select('#xlabeltext').text('Sorted from largest to smallest Emission');
    }
    x.domain(current.map(d => d.country));
    sortMode = mode;
}

function toggleSort(id) {
    d3.selectAll('.sort')
        .style('background-color', '#eee');
    d3.select(id)
        .style('background-color', 'lightblue'); // #add8e6 is hex code for lightblue
}

function toggleFilter(id) {
    d3.selectAll('.filter')
        .style('background-color', '#eee');
    d3.select(id)
        .style('background-color', 'lightblue'); // #add8e6 is hex code for lightblue
}

///////////////////////////////////////////////////////////////
// draw the chart
///////////////////////////////////////////////////////////////

var x = d3.scaleBand();
var y = d3.scaleLinear();

var delay = function (d, i) {
    return i * 50;
};

function redraw() {
    //update scale
    x.domain(current.map(d => d.country));

    ////////////////////////////////
    // DATA JOIN FOR BARS.
    var bars = svg.selectAll('.bar')
        .data(current, d => d.country);

    // UPDATE.
    bars.transition()
        .duration(750)
        .delay(delay)
        .attr('x', d => x(d.country))
        .attr('width', x.bandwidth());

    // ENTER.
    bars.enter()
        .append('rect')
        .attr('x', d => x(d.country))
        .attr('y', d => y(0))
        .attr('width', x.bandwidth())
        .transition()
        .duration(750)
        .attr('class', 'bar')
        .attr('x', d => x(d.country))
        .attr('y', d => y(d.co2_per_capita))
        .attr('width', x.bandwidth())
        .attr('height', d => height - y(d.co2_per_capita));

    // EXIT.
    bars.exit()
        .transition()
        .duration(750)
        .style('opacity', 0)
        .remove();

    ////////////////////////////////
    // DATA JOIN FOR NAMES.
    var name = svg.selectAll('.name')
        .data(current, d => d.country);

    // UPDATE.
    name.transition()
        .duration(750)
        .delay(delay)
        .attr('x', (d, i) => x(d.country) + x.bandwidth() / 2);

    // ENTER.
    name.enter()
        .append('text')
        .attr('x', d => x(d.country) + x.bandwidth() / 2)
        .attr('y', d => y(d.co2_per_capita) + (height - y(d.co2_per_capita)) / 2)
        .style('opacity', 0)
        .transition()
        .duration(1000)
        .text(d => d.country)
        .attr('class', 'name')
        .attr('style', d => d.country === 'EARTH' ? 'fill: red' : '')
        .attr('x', d => x(d.country) + x.bandwidth() / 2)
        .attr('y', d => y(d.co2_per_capita) + (height - y(d.co2_per_capita)) / 2)
        .style('opacity', 1);

    // EXIT.
    name.exit()
        .transition()
        .duration(750)
        .style('opacity', 0)
        .remove();
}

function transition() {
    var transition = svg.transition()
        .duration(750);

    transition.selectAll('.bar')
        .delay(delay)
        .attr('x', d => x(d.country));

    transition.selectAll('.name')
        .delay(delay)
        .attr('x', d => x(d.country) + x.bandwidth() / 2);
}

function draw() {
    x.domain(current.map(d => d.country))
        .range([0, width])
        .paddingInner(0.2);

    y.domain([0, d3.max(current, d => d.co2_per_capita)])
        .range([height, 0]);

    svg.selectAll('.bar')
        .data(current, d => d.name)
        .enter()
        .append('rect')
        .attr('class', 'bar')
        .attr('x', d => x(d.country))
        .attr('y', d => y(d.co2_per_capita))
        .attr('width', x.bandwidth())
        .attr('height', d => height - y(d.co2_per_capita));

    svg.selectAll('.name')
        .data(current, d => d.country)
        .enter()
        .append('text')
        .text(d => d.country)
        .attr('class', 'name')
        .attr('x', d => x(d.country) + x.bandwidth() / 2)
        .attr('y', d => y(d.co2_per_capita) + (height - y(d.co2_per_capita)) / 2);

    var xAxis;
    xAxis = d3.axisBottom()
        .scale(x)
        .ticks(0)
        .tickSize(0)
        .tickFormat('');

    svg.append('g')
        .attr('class', 'axis')
        .attr('transform', 'translate(0,' + height + ')')
        .call(xAxis);

    svg.append('text')
        .attr('x', width / 2)
        .attr('y', height + 20)
        .attr('class', 'label')
        .attr('id', 'xlabeltext')
        .text('Sorted Alphabetically by country Name');

    var yAxis = d3.axisLeft()
        .scale(y)
        .ticks(5, 'd')
        .tickFormat(d3.format(".2s"));

    svg.append('g')
        .attr('class', 'axis')
        .call(yAxis);

    svg.append('text')
        .attr('x', - height / 2)
        .attr('y', - margin.left * 0.7)
        .attr('transform', 'rotate(-90)')
        .attr('class', 'label')
        .append('tspan').text('CO2 Emission (kt)')
}