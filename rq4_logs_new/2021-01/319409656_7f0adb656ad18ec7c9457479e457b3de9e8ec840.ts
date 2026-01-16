import { Component, OnInit } from '@angular/core';
import { AuthService } from 'src/app/auth/shared/auth.service';
import { IPublishingRequest } from 'src/app/DTO/ipublishing-request';
import { ChiefEditorService } from '../shared/chief-editor.service';

@Component({
  selector: 'app-check-if-original-list',
  templateUrl: './check-if-original.component-list.html',
  styleUrls: ['./check-if-original.component-list.css']
})
export class CheckIfOriginalList implements OnInit {

  requests: IPublishingRequest[] = [];

  constructor(private editorService:ChiefEditorService, private authService:AuthService) { 
    this.refreshTable();
  }

  ngOnInit(): void {

    this.refreshTable();
  }

  refreshTable() {
    var logged=this.authService.getLoggedUser();
		//this.editorService.getListOfPossibleSources(logged).subscribe((data: IPublishingRequest[]) =>{this.requests = data; console.log(data)} );
	}
}