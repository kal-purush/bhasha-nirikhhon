var stations = [
  '南港展覽館站',
  '南港站',
  '昆陽站',
  '後山埤站',
  '永春站',
  '市政府站',
  '國父紀念館站',
  '忠孝敦化站',
  '忠孝復興站',
  '忠孝新生站',
  '善導寺站',
  '台北車站',
  '西門站',
  '龍山寺站',
  '江子翠站',
  '新埔站',
  '板橋站',
  '府中站',
  '亞東醫院站',
  '海山站',
  '土城站',
  '永寧站',
  '頂埔站'
];

var nodesByName = {};

// set the dimensions and margins of the diagram
var margin = {
    top: -90,
    right: 90,
    bottom: 30,
    left: 90
  },
  width = 1024 - margin.left - margin.right,
  height = 300 - margin.top - margin.bottom;

var treeData = {};

var treemap = d3.tree().size([height, width]);

var setNode = function (node, name, child) {
  if (node.children) {
    setNode(node.children[0], name, child);
  } else {
    node['name'] = name;
    node['children'] = [{
      name: child
    }];
  }
}

for (var i = 0; i < stations.length - 1; i++) {
  setNode(treeData, stations[i], stations[i + 1]);
}


var nodes = d3.hierarchy(treeData);

nodes = treemap(nodes);

var svg = d3.select('svg')
  .attr('width', width + margin.left + margin.right)
  .attr('height', height + margin.top + margin.bottom),
  g = svg.append('g')
  .attr('transform',
    'translate(' + margin.left + ',' + margin.top + ')');


var link = g.selectAll('.link')
  .data(nodes.descendants().slice(1))
  .enter().append('path')
  .attr('class', 'link')
  .attr('d', function (d) {
    return 'M' + d.y + ',' + d.x +
      'C' + (d.y + d.parent.y) / 2 + ',' + d.x +
      ' ' + (d.y + d.parent.y) / 2 + ',' + d.parent.x +
      ' ' + d.parent.y + ',' + d.parent.x;
  });

var node = g.selectAll('.node')
  .data(nodes.descendants())
  .enter().append('g')
  .attr('class', function (d) {
    return 'node' +
      (d.children ? ' node--internal' : ' node--leaf');
  })
  .attr('transform', function (d) {
    return 'translate(' + d.y + ',' + d.x + ')';
  });


node.append('circle')
  .attr('r', 5);

node.append('text')
  .attr('dy', '5em')
  .style('text-anchor', 'middle')
  .style('writing-mode', 'tb')
  .text(function (d) {
    return d.data.name;
  });