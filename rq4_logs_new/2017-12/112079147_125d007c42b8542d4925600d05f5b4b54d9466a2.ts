import {Component, Inject, OnInit} from '@angular/core';
import {FrontendSelectorsIds} from '../../globals/frontend-selectors-ids';
import {MAT_SNACK_BAR_DATA} from '@angular/material';
import {AlertSnackbarData} from '../../models/alert-snackbar-data.model';
import {MaterialsColorsIds} from '../../../../globals/materials-colors-ids';

@Component({
  selector: FrontendSelectorsIds.AlertPopupSelector,
  templateUrl: './alert-popup.component.html',
  styleUrls: ['./alert-popup.component.less']
})
export class AlertPopupComponent implements OnInit {

  color: string;
  message: string;

  constructor(@Inject(MAT_SNACK_BAR_DATA) public data: AlertSnackbarData) {
  }

  ngOnInit() {
    this.color = MaterialsColorsIds.Accent;
    this.message = this.data.message;
  }
}