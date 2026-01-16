import { Component, OnInit } from '@angular/core';
import { AuthService } from 'src/app/auth/shared/auth.service';
import { IPublishingRequest } from 'src/app/DTO/ipublishing-request';
import { LectorService } from '../shared/lector.service';

@Component({
  selector: 'app-lector-requests-list',
  templateUrl: './lector-requests-list.component.html',
  styleUrls: ['./lector-requests-list.component.css']
})
export class LectorRequestsListComponent implements OnInit {
  requests: IPublishingRequest[] = [];

  constructor(private authService:AuthService, private lectorService:LectorService) { }

  ngOnInit(): void {
    this.refreshTable();
  }

  refreshTable() {
    var logged=this.authService.getLoggedUser();
		this.lectorService.getRequests(logged).subscribe((data: IPublishingRequest[]) =>{this.requests = data; } );
	}

}