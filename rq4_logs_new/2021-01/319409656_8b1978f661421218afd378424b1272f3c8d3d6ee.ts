import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { IFile } from 'src/app/DTO/ifile';
import { IPublishingRequest } from 'src/app/DTO/ipublishing-request';
import { FormService } from 'src/app/form/shared/form.service';
import { LectorService } from '../shared/lector.service';

@Component({
    selector: 'app-lector-request',
    templateUrl: './lector-request.component.html',
    styleUrls: ['./lector-request.component.css']
})
export class LectorRequestComponent implements OnInit {
    publishingRequest: IPublishingRequest;
    processInstanceId: string;
    dataLoaded:boolean=false;

    constructor(private router: Router,
         private activatedRoute: ActivatedRoute,
          private lectorService: LectorService,
          private formService:FormService) { }

    ngOnInit(): void {
        this.activatedRoute.paramMap.subscribe((params) => {
            this.getRequest(params.get('id'));
        });
    }

    getRequest(id: string) {
        this.lectorService.getRequest(id).subscribe(
            (data: IPublishingRequest) => {
                this.publishingRequest = data;
            },
            (error) => alert(error.error)
        );
        this.formService.getProcessInstanceId(id, "publishingRequestId").subscribe(
            (data) => {
                this.processInstanceId = String(data.processId);
                this.dataLoaded= true;
            },
            (error) => alert(error.error)
        );
    }

    download(file: IFile) {
        this.lectorService.getDocument(file.url).subscribe((data) => console.log(data));
    }

    showForm() {
        switch (this.publishingRequest.status) {
            case 'Book is sent to lector':
                return true;
            default:
                return false;
        }
    }

}