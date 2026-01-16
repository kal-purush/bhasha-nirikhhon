import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';

@Component({
  selector: 'app-list-item',
  templateUrl: './list-item.component.html',
  styleUrls: ['./list-item.component.scss']
})
export class ListItemComponent implements OnInit {

  @Input() id!: string;
  @Input() content!: string;
  @Input() data!: any;


  @Output() onDelete: EventEmitter<any> = new EventEmitter();
  @Output() onUpdate: EventEmitter<any> = new EventEmitter();

  constructor() { }

  ngOnInit(): void {
  }

  delete(): void {
    console.log("Delete");
    this.onDelete.emit([this.data]);
  }

  update(): void {
    console.log("Update");
    this.onUpdate.emit([this.data]);
  }

}