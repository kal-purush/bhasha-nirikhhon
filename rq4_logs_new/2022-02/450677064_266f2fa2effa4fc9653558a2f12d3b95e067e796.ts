import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-evalation-question',
  templateUrl: './evalation-question.component.html',
  styleUrls: ['./evalation-question.component.scss']
})
export class EvalationQuestionComponent implements OnInit {

  question:any = {
    contenu : "Which gender are you ",
    noTypeQuestion : 1,
    aide : "HERE AAAAA1",
    reponses : [
      {
        checked : false,
        contenu : "Are you gay"
      },
      {
        checked : true,
        contenu : "Are you hetero"
      },
      {
        checked : false,
        contenu : "Are you Lesbienne"
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