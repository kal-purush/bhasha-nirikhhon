import { Component, Inject} from '@angular/core';
import { MD_DIALOG_DATA } from '@angular/material';

@Component({
  selector: 'app-mydialog',
  templateUrl: 'mydialog.component.html',
})
export class MydialogComponent {
  constructor( @Inject(MD_DIALOG_DATA) public data: any) { }
}