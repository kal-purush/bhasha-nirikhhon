import { Component, OnInit } from '@angular/core';
import { ICellRendererAngularComp } from 'ag-grid-angular/main';
import { ICellRendererParams } from 'ag-grid-community';

@Component({
  selector: 'app-dropdown',
  templateUrl: './dropdown.component.html',
  styleUrls: ['./dropdown.component.css']
})
export class DropdownComponent implements OnInit {
  public params: ICellRendererParams;

  agInit(params: ICellRendererParams): void {
    this.params = params;
  }
  constructor() { }

  ngOnInit() {
  }

}