import { Component, OnInit, Input } from '@angular/core';

@Component({
  selector: 'app-question-workspace',
  templateUrl: './question-workspace.component.html',
  styleUrls: ['./question-workspace.component.scss']
})
export class QuestionWorkspace implements OnInit {
  @Input() newQuestion: any;
  optionCount = 3;
  constructor() {

  }
  ngOnInit() {

  }
  addNewOption() {
    this.newQuestion.options.push(
      {
        value: "option"+this.optionCount,
        label: "option"+this.optionCount
      }
    );
    this.optionCount++;
  }
}