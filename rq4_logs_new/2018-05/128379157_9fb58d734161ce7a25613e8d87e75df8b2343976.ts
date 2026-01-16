import { Component, Inject } from '@angular/core';
import { MatDialog, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material';
import { FormControl, Validators } from '@angular/forms';
import { QuestionService } from '../../services/question.service';

@Component({
  selector: 'app-question-dialog',
  templateUrl: './question-dialog.component.html',
  styleUrls: ['./question-dialog.component.css']
})
export class QuestionDialogComponent {
  label = new FormControl(this.data.label, [Validators.required]);
  choices: any[] = [{label: 'Option1'}, {label: 'Option2'}, {label: 'Option3'}];

  constructor(
    public dialogRef: MatDialogRef<QuestionDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any,
    private questionService: QuestionService
  ) { }


  handleSubmit(): void {
    if (this.label.invalid) {
      return;
    }

    if (this.data.id) {
      return this._update();
    }

    this._create();
  }

  _update() {
    this.questionService.update(this.data.story_id, this.data.id, this._buildData())
      .subscribe(
        res => {
          this.dialogRef.close(res);
        },
        err => {
          console.log(err);
        }
      );
  }

  _create() {
    this.questionService.create(this.data.story_id, this._buildData())
      .subscribe(
        res => {
          this.dialogRef.close(res);
        },
        err => {
          console.log(err);
        }
      );
  }

  _buildData() {
    let obj = {
      id: this.data.id,
      label: this.label.value,
      story_id: this.data.story_id,
    }

    return {
      question: obj
    }
  }
}