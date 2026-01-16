import { Component, OnInit, EventEmitter, Output, Input } from '@angular/core';

@Component({
  moduleId: module.id,
  selector: 'tree-node',
  templateUrl: 'tree-node.component.html',
  styleUrls: ['tree-node.component.css']
})

export class TreeNodeComponent implements OnInit {
  @Input() name: string;
  @Input() value: boolean;
  @Output() checked: EventEmitter<any> = new EventEmitter();

  constructor() { }

  ngOnInit() {
    console.log('tree-node rendered');
  }

  checkChanged() {
    this.checked.emit(this.value);
  }

}