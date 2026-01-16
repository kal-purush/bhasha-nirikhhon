import { Component, OnInit } from '@angular/core';
import { ActivatedRoute, Router } from '@angular/router';
import { IPublishingRequest } from 'src/app/DTO/ipublishing-request';
import { BookDTO } from 'src/app/DTO/book-dto';
import { ChiefEditorService } from '../shared/chief-editor.service';
import { IFile } from 'src/app/DTO/ifile';

@Component({
  selector: 'app-read-book',
  templateUrl: './read-book.component.html',
  styleUrls: ['./read-book.component.css']
})
export class ReadBookComponent implements OnInit {
    publishingRequest :IPublishingRequest;

    constructor(private router: Router, private activatedRoute: ActivatedRoute, private chiefEditorService: ChiefEditorService) { }

    ngOnInit(): void {
        this.activatedRoute.paramMap.subscribe((params) => {
			this.getRequest(params.get('id'));
		});
    }

    getRequest(id: string) {
		this.chiefEditorService.getRequest(id).subscribe(
			(data: IPublishingRequest) => {
				this.publishingRequest = data;
			},
			(error) => alert(error.error)
		);
    }

    download(file: IFile) {
      this.chiefEditorService.getDocument(file.url).subscribe((data) => console.log(data));
    }

    accept() {
      let body={
        publishingRequestId:this.publishingRequest.id,
        response:true
      };

      this.chiefEditorService.acceptBook(body).subscribe(()=>
        {
          //this.router.navigateByUrl('editor/chief-editor-plagiarism-requests');
        },
        (error)=> alert(error.console.error)
      );
    }

    refuse() {
      let body={
        publishingRequestId: this.publishingRequest.id,
        response: false
      };

      this.chiefEditorService.acceptBook(body).subscribe(()=>
        {
          //this.router.navigateByUrl('/refusal/'+this.publishingRequest.id)
        },
        (error)=> alert(error.console.error)
      );
    }
}