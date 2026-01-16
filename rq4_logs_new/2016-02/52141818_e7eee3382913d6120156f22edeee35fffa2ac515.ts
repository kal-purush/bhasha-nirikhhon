///<reference path="../core/Grid.ts"/>
///<reference path="../../typings/main.d.ts"/>
import {Grid} from "../core/Grid";
import {Node} from "../core/Graph";
import {HtmlGridRenderer} from "./GridRenderer";

import React = require('react');
import ReactDom = require('react-dom');

const n = 6;

const grid = new Grid(n, n);

var region = grid.DetermineAllRegions([18, 19, 20, 14, 8, 2].map(n => grid.Graph().NodeAt(n)));
console.log(region);

var region = grid.DetermineAllRegions([6, 7, 1, 2, 3, 4, 10, 11].map(n => grid.Graph().NodeAt(n)));
console.log(region);

//grid.Graph().DeleteNodeAt(0);

ReactDom.render(React.createElement(HtmlGridRenderer, {
    grid: grid,
    solution: [0, 1, 7, 13, 19, 25, 31, 32, 26, 20, 14, 8, 2, 3, 4, 5, 11, 10, 16, 17, 23, 22, 28, 29].map(n => grid.Graph().NodeAt(n)),
    cellWidth: 50,
    cellMargin: 2
}), document.getElementById('grid'));