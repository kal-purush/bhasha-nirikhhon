import { Component, OnInit } from '@angular/core';
import {ChiefEditorService} from '../shared/chief-editor.service';
import {IComplaint} from '../../DTO/icomplaint';

@Component({
  selector: 'app-complaints',
  templateUrl: './complaints.component.html',
  styleUrls: ['./complaints.component.css']
})
export class ComplaintsComponent implements OnInit {
  complaints: IComplaint[];

  constructor(private chiefEditorService: ChiefEditorService) { }

  ngOnInit(): void {
    this.chiefEditorService.getComplaints().subscribe(
      data => this.complaints = data
    );
  }

}