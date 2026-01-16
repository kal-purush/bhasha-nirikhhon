import {Component, View, ViewEncapsulation, OnInit} from 'angular2/core';
import * as d3 from 'd3';

import {Result} from '../interfaces/docker-container';

@Component({
  selector: 'containers-graph'
})

@View({
  template: `
    <div id='containerGraph'>
      <svg>
        <defs>
          <linearGradient id="linear" x1="0%" y1="0%" x2="100%" y2="0%">
            <stop offset="0%" stop-color="#2ef2dc"/>
            <stop offset="100%" stop-color="#2bd9f8"/>
          </linearGradient>
        </defs>
      </svg>
    </div>
  `,
  styles: [
    require('./containers-graph.directive.css')
  ],
  encapsulation: ViewEncapsulation.None,
  directives: [ContainersGraph]
})

export class ContainersGraph implements OnInit {
  ngOnInit() {
    //var rect = d3.select('#containerGraph').node().getBoundingClientRect();
    var diameter = 1080; //rect.width;
    var radius = diameter / 2;
    var innerRadius = radius - 180;

    var cluster = d3.layout.cluster<Result>()
      .size([360, innerRadius])
      .sort(null);

    var bundle = d3.layout.bundle<Result>();

    var line = d3.svg.line.radial<Result>()
      .interpolate('bundle')
      .tension(0.85)
      .radius((d) => d.y)
      .angle((d) => d.x / 180 * Math.PI);

    var svg = d3.select('#containerGraph svg')
      .attr('width', diameter)
      .attr('height', diameter)
      .attr('viewBox', `0 0 ${diameter} ${diameter}`)
      .attr('preserveAspectRatio', 'xMinYMin')
      .append('g')
      .attr('transform', `translate(${radius},${radius})`);

    var link = svg.append('g').selectAll('.link');
    var node = svg.append('g').selectAll('.node');

    var packages = {
        root: function(items: any[]) {
          var map: {[key: string]: Result} = {};
          function find(id: string, data?: any) {
            var node: Result = map[id];
            var i: number;
            if (!node) {
              node = map[id] = data || {
                id: id,
                children: []
              };

              if (id.length) {
                node.parent = find(id.substring(0, i = id.lastIndexOf('.')));
                node.parent.children.push(node);
                node.type = data.type;
                if (data.names.length && data.names[0] !== '<none>:<none>') {
                  node.key = data.names[0];
                }
                if (!node.key) {
                  node.key = id.substr(0, 12);
                }
              }
            }
            return node;
          }
          items.forEach((d) => find(d.id, d));
          return map[''];
        },

        images: function(nodes: any[]) {
          var map: {[key: string]: Result} = {},
            images: d3.layout.cluster.Link<Result>[] = [];

          nodes.forEach((d) => map[d.id] = d);
          nodes.forEach((d) => {
            if (d.images) d.images.forEach((i: string) => {
              if (i !== '') {
                images.push({
                  source: map[d.id],
                  target: map[i]
                });
              }
            });
          });

          return images;
        }
    };

    d3.json('/api/v1/docker/containers/graph', function(error, items) {
      if (error) throw error;

      var nodes = cluster.nodes(packages.root(items));
      var links = packages.images(nodes);

      link = link.data(bundle(links))
        .enter()
        .append('path')
        .attr('class', 'link')
        .attr('d', line);

      node = node.data(nodes.filter((n) => !n.children))
        .enter()
        .append('g')
        .attr('class', 'node')
        .attr('transform', (d) => `rotate(${d.x - 90}) translate(${d.y})`)
        .append('text')
        .attr('dx', (d) => d.x < 180 ? 8 : -8)
        .attr('dy', '0.31em')
        .attr('text-anchor', (d) => d.x < 180 ? 'start' : 'end')
        .attr('transform', (d) => d.x < 180 ? null : 'rotate(180)')
        .text((d) => {
          var key = d.key.replace(/^\//, '');
          if (d.type === 'container') {
            var spl = key.split(/\//g);
            return spl[spl.length - 1];
          }
          return key;
        })
        .on('mouseover', mouseover)
        .on('mouseout', mouseout);
    });

    function highlightTarget(d) {
      link.classed('link--target', (l) => {
        var source = l[0];
        var target = l[l.length - 1];
        if (source.source) return true;

        if (target === d) {
          highlightTarget(source);
          return source.source = true;
        }
      });
    }

    function highlightSource(d) {
      link.classed('link--source', (l) => {
        var source = l[0];
        var target = l[l.length - 1];
        if (target.target) return true;

        if (source === d) {
          highlightSource(target);
          return target.target = true;
        }
      });
    }

    function mouseover(d) {
      node.each((n) => n.target = n.source = false);

      highlightTarget(d);
      highlightSource(d);

      link.filter((l) => l.target === d || l.source === d)
        .each(() => this.parentNode.appendChild(this));

      node.classed('node--target', (n) => n.target)
        .classed('node--source', (n) => n.source);
    }

    function mouseout(d) {
      link.classed('link--target', false)
        .classed('link--source', false);

      node.classed('node--target', false)
        .classed('node--source', false);
    }
  }
}