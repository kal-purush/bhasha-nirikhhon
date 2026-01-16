/*-----------------------------------------------------------------------------
| Copyright (c) 2014-2015, PhosphorJS Contributors
|
| Distributed under the terms of the BSD 3-Clause License.
|
| The full license is in the file LICENSE, distributed with this software.
|----------------------------------------------------------------------------*/
'use-strict';

import {
  Widget, attachWidget
} from 'phosphor-widget';

import {
  DashboardPanel
} from '../lib/index';

import './index.css';


function createContent(name: string): Widget {
  var widget = new Widget();
  widget.addClass('content');
  widget.addClass(name);
  return widget;
}


function main(): void {
  var r1 = createContent('red');
  var g1 = createContent('green');
  var b1 = createContent('blue');
  var y1 = createContent('yellow');

  var r2 = createContent('red');
  var g2 = createContent('green');
  var b2 = createContent('blue');
  var y2 = createContent('yellow');

  var r3 = createContent('red');
  var g3 = createContent('green');
  var b3 = createContent('blue');

  DashboardPanel.setRow(r1, 0);
  DashboardPanel.setColumn(r1, 0);
  DashboardPanel.setRowSpan(r1, 2);
  DashboardPanel.setColumnSpan(r1, 4);

  DashboardPanel.setRow(g1, 0);
  DashboardPanel.setColumn(g1, 4);
  DashboardPanel.setRowSpan(g1, 4);
  DashboardPanel.setColumnSpan(g1, 4);

  DashboardPanel.setRow(b1, 0);
  DashboardPanel.setColumn(b1, 8);
  DashboardPanel.setRowSpan(b1, 2);
  DashboardPanel.setColumnSpan(b1, 2);

  DashboardPanel.setRow(y1, 0);
  DashboardPanel.setColumn(y1, 10);
  DashboardPanel.setRowSpan(y1, 2);
  DashboardPanel.setColumnSpan(y1, 2);

  DashboardPanel.setRow(r2, 2);
  DashboardPanel.setColumn(r2, 0);
  DashboardPanel.setRowSpan(r2, 2);
  DashboardPanel.setColumnSpan(r2, 2);

  DashboardPanel.setRow(g2, 2);
  DashboardPanel.setColumn(g2, 2);
  DashboardPanel.setRowSpan(g2, 4);
  DashboardPanel.setColumnSpan(g2, 2);

  DashboardPanel.setRow(b2, 2);
  DashboardPanel.setColumn(b2, 8);
  DashboardPanel.setRowSpan(b2, 2);
  DashboardPanel.setColumnSpan(b2, 4);

  DashboardPanel.setRow(y2, 4);
  DashboardPanel.setColumn(y2, 0);
  DashboardPanel.setRowSpan(y2, 2);
  DashboardPanel.setColumnSpan(y2, 2);

  DashboardPanel.setRow(r3, 4);
  DashboardPanel.setColumn(r3, 4);
  DashboardPanel.setRowSpan(r3, 2);
  DashboardPanel.setColumnSpan(r3, 4);

  DashboardPanel.setRow(g3, 4);
  DashboardPanel.setColumn(g3, 8);
  DashboardPanel.setRowSpan(g3, 2);
  DashboardPanel.setColumnSpan(g3, 2);

  DashboardPanel.setRow(b3, 4);
  DashboardPanel.setColumn(b3, 10);
  DashboardPanel.setRowSpan(b3, 2);
  DashboardPanel.setColumnSpan(b3, 2);

  var panel = new DashboardPanel();
  panel.id = 'main';
  panel.maxRowSize = 100;

  panel.children = [r1, g1, b1, y1, r2, g2, b2, y2, r3, g3, b3];

  attachWidget(panel, document.body);

  window.onresize = () => panel.update();
}


window.onload = main;