import {Component, Inject, OnInit} from '@angular/core';
import {MatDialogRef, MAT_DIALOG_DATA, MatSnackBar} from '@angular/material';
import {TitleService} from '../../../../shared/services/title.service';

@Component({
  selector: 'app-upload-track',
  templateUrl: './upload-track.component.html',
  styleUrls: ['./upload-track.component.css']
})
export class UploadTrackComponent implements OnInit {

  constructor(private snackBar: MatSnackBar, private titleService: TitleService,
              private dialogRef: MatDialogRef<UploadTrackComponent>,
              @Inject(MAT_DIALOG_DATA) public data: any) {
  }

  ngOnInit() {
    this.titleService.setTitleWithOutConstant('ادمین: بارگذاری فایل');
  }

  Success($event: Event) {

    this.snackBar.open('File is uploaded successfully', null, {
      duration: 2000,
    });

  }
}