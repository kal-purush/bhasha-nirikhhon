import { Component, OnInit } from '@angular/core';
import { OverlayService } from '../overlay/overlay.module';
import { SurveyService } from '../services/survey/survey.service';
import { MatSnackBar, MatDialog } from '@angular/material';
import { ActivatedRoute, Router } from '@angular/router';

@Component({
  selector: 'app-success',
  templateUrl: './success.component.html',
  styleUrls: ['./success.component.scss']
})

export class SuccessComponent implements OnInit {
  sharinglink = '';
  type = '';
  constructor(private _overlayService: OverlayService, private _snackBar: MatSnackBar, public dialog: MatDialog, private _activateRoute: ActivatedRoute) {
    this._overlayService.show();
    this._activateRoute.params.subscribe((data) => {
      let routeGuid = data['id'];
      this.type = data['type'];

      switch (this.type) {
        case 'survey':
          this.sharinglink = this.generateSurveyLink(routeGuid);
          break;
        case 'poll':
          this.sharinglink = this.generatePollLink(routeGuid);
          break;
        default:
          this.sharinglink = this.generateSurveyLink(routeGuid);
          break;
      }
      this._overlayService.hide();
    });
  }

  ngOnInit() { }

  generatePollLink(shareId: string): string {
    return window.location.origin + `/poll/view/${shareId}`;
  }

  generateSurveyLink(shareId: string): string {
    return window.location.origin + `/survey/view/${shareId}`;
  }

  onCopyClick() {
    let selBox = document.createElement('textarea');
    selBox.style.position = 'fixed';
    selBox.style.left = '0';
    selBox.style.top = '0';
    selBox.style.opacity = '0';
    selBox.value = this.sharinglink;
    document.body.appendChild(selBox);
    selBox.focus();
    selBox.select();
    document.execCommand('copy');
    document.body.removeChild(selBox);
    this._snackBar.open('Copied to clipboard', 'Dismiss', {
      duration: 3000,
    });
  }

}