import {Component} from 'angular2/core';
import {TreeComponent} from 'ng2-treeview';

import {TREEDATA} from './mock-tree-data'

@Component({
  selector: 'app',
  template:`<tree-view [tree]="tree"></tree-view>`,
  directives: [TreeComponent]
})
export class AppComponent {
  public tree = TREEDATA;
}