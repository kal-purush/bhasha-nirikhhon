import { Component, OnInit } from '@angular/core';
import { AuthService } from 'src/app/auth/shared/auth.service';
import { IPublishingRequest } from 'src/app/DTO/ipublishing-request';
import { ChiefEditorService } from '../shared/chief-editor.service';

@Component({
  selector: 'app-read-books-list',
  templateUrl: './read-books-list.component.html',
  styleUrls: ['./read-books-list.component.css']
})
export class ReadBooksListComponent implements OnInit {

  requests: IPublishingRequest[] = [];

  constructor(private editorService:ChiefEditorService, private authService:AuthService) { 
    this.refreshTable();
  }

  ngOnInit(): void {

    this.refreshTable();
  }

  refreshTable() {
    var logged=this.authService.getLoggedUser();
		this.editorService.getListOfBooksToRead(logged).subscribe((data: IPublishingRequest[]) =>{this.requests = data; console.log(data)} );
	}
}