import { Component, Input } from '@angular/core';
import { Node } from '../model/model';
import {TreeService} from '../services/tree.service';

@Component({
    selector: 'ez-node',
    templateUrl: './node.component.html'
})
export class NodeComponent {

    @Input() node: Node;

    constructor(private treeService: TreeService){}

    toggle(){
        if (this.node.Expanded){
            this.treeService.nodeCollapsed.emit(this.node);
        } else {
            this.treeService.nodeExpanded.emit(this.node);
        }
        this.node.Expanded = !this.node.Expanded;
    }
}