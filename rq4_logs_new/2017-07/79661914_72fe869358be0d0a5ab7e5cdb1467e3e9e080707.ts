import { Component, Input } from '@angular/core';
import { environment } from '../../../environments/environment';

@Component({
  moduleId: module.id,
  selector: 'app-attachments',
  templateUrl: 'attachments.component.html',
  styleUrls: ['attachments.component.css']
})
export class AttachmentsComponent  {

  @Input() attachments: any[];
}