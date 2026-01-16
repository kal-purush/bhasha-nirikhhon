import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { IPublishingRequest } from 'src/app/DTO/ipublishing-request';
import { BookDTO } from 'src/app/DTO/book-dto';
import { ChiefEditorService } from '../shared/chief-editor.service';
import { IFile } from 'src/app/DTO/ifile';
import { IUser } from 'src/app/DTO/iuser';

@Component({
  selector: 'app-choose-beta-readers',
  templateUrl: './choose-beta-readers.component.html',
  styleUrls: ['./choose-beta-readers.component.css']
})
export class ChooseBetaReadersComponent implements OnInit {
    publishingRequest :IPublishingRequest;
    requestId: number;
    availableUsers: Array<IUser>;

    constructor(private router: Router, private activatedRoute: ActivatedRoute, private chiefEditorService: ChiefEditorService) { }

    ngOnInit(): void {
        this.activatedRoute.paramMap.subscribe((params) => {
            //this.getRequest(params.get('id'));
            this.requestId = parseInt(params.get('id'));
        });
        this.availableUsers = new Array<IUser>();
    }

}