
import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { Router } from '@angular/router';
import { SurveyModel } from 'src/app/create-survey/create-survey.component';
import { SurveyService } from '../services/survey/survey.service';
import { OverlayService } from '../overlay/overlay.service';
import { MatSnackBar } from '@angular/material/snack-bar';


@Component({
    selector: 'app-search-survey',
    templateUrl: './search-survey.component.html',
    styleUrls: ['./search-survey.component.scss']
  })

  export class SearchSurveyComponent implements OnInit{
    surveySearchText = '';
  surveyExists = false;
  constructor(private _router: Router, private _surveyService: SurveyService, private _snackBar: MatSnackBar,
    private _overlayService: OverlayService) { }
  ngOnInit() {
  
  }

  viewSurvey() {
    this.checkDetails('view');
  }

  reviewSurvey() {
    this.checkDetails('result');
  }
    
  checkDetails(type: string) {
    this._overlayService.show();
    this._surveyService.getSurvey(this.surveySearchText).subscribe(
      (data: SurveyModel) => {
        this._overlayService.hide();
        this.surveyExists = true;
        this._router.navigate([`survey/${type}/` + this.surveySearchText]);
      },
      error => {
        this.surveyExists = false;
        this._overlayService.hide();
        switch (error.error) {
          case 'SurveyNotFound':
            this.openDismiss('Invalid Survey', 'Dismiss');
            break;
          case 'SurveyEnded':
            if (type === 'result') {
              this._router.navigate([`survey/${type}/` + this.surveySearchText]);
              break;
            }
            this.openDismiss('The Survey you are looking for ended', 'Dismiss');
            break;
          default:
            this.openDismiss('Something went wrong', 'Dismiss');
            break;
        }
      });
  }
  
   // open snackbar
   openDismiss(message: string, buttontext: string) {
    this._snackBar.open(message, buttontext, {
      duration: 3000,
    });
  }
  }