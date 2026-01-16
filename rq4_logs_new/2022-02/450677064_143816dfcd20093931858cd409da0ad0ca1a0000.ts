import { Component, Input, OnInit } from "@angular/core";
import { QuestionType } from "@/interfaces/QuestionType";

@Component({
  selector: 'app-evaluation-question',
  templateUrl: './evaluation-question.component.html',
  styleUrls: ['./evaluation-question.component.scss']
})
export class EvaluationQuestionComponent implements OnInit {
  @Input() questionType: QuestionType;

  readonly QuestionType=QuestionType;

  question:any = {
    contenu : "Which gender are you ",
    noTypeQuestion : 1,
    aide : "HERE AAAAA1",
    reponses : [
      {
        checked : false,
        contenu : "Are you Female"
      },
      {
        checked : true,
        contenu : "Are you Male"
      },
      {
        checked : false,
        contenu : "I don't wanna answer question"
      }
    ]
  }

  q2:any = {
    contenu : "Quel est le nom de l'entreprise",
    noTypeQuestion : 4,
    aide : "HERE BBBBBBBBBBBB1"
  }

  constructor() { }

  ngOnInit(): void {
  }

}