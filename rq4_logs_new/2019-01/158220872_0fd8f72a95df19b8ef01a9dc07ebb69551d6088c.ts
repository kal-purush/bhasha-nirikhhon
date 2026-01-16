import { Component, OnInit } from '@angular/core';
import {CdkDragDrop, moveItemInArray, transferArrayItem, copyArrayItem} from '@angular/cdk/drag-drop';

@Component({
  selector: 'app-add-question',
  templateUrl: './add-question.component.html',
  styleUrls: ['./add-question.component.scss']
})
export class AddQuestionComponent implements OnInit {
  flag = true;
  newQuestion ={
    _id: "5be8e8582d325f5b71da4e08",
    question: [
        "Total students assessed in Mathematics-",
        ""
    ],
    options: [],
    children: [],
    questionGroup: [
        "A1"
    ],
    fileName: [],
    instanceQuestions: [],
    deleted: false,
    tip: "",
    externalId: "AS/TL/04a",
    visibleIf: "",
    file: "",
    responseType: "number",
    validation: {
        required: true,
        isNumber: true,
        regex: "^[0-9s]*$"
    },
    showRemarks: false,
    isCompleted: false,
    remarks: "",
    value: "",
    canBeNotApplicable: "false",
    usedForScoring: "",
    modeOfCollection: "onfield",
    questionType: "auto",
    accessibility: "local",
    updatedAt: "2018-11-12T02:41:28.851Z",
    createdAt: "2018-11-12T02:41:28.851Z",
    __v: 0,
    evidenceMethod: "AC3",
    payload: {
        criteriaId: "5be180b05e852b0e920ad156",
        responseType: "number",
        evidenceMethod: "AC3"
    },
    startTime: "",
    endTime: ""
  };
  constructor() { }

  ngOnInit() {
  }

  // todo = [
  //   'radio',
  //   'matrix',
  //   'text',
  //   'number',
  //   'textarea',
  //   'select',

  // ];

  // done = [
    
  // ];


  // drop(event: CdkDragDrop<string[]>) {

  //   console.log(event)
  //   if (event.previousContainer === event.container) {
  //     moveItemInArray(event.container.data, event.previousIndex, event.currentIndex);
  //   } 
  //     copyArrayItem(event.previousContainer.data,
  //       event.container.data,
  //       event.previousIndex,
  //       event.currentIndex);
     
    
  // }
  // noReturnPredicate() {
  //   return false;
  // }

  // evenPredicate() {
  //     return false;
      
  // }
  currentQuestion:string;
  questionType=[
    'radio',
    'text',
    'number',
    'textarea',
    'select',
  ]
  openQuestion(question:string){
    this.currentQuestion = question ;
  }

}