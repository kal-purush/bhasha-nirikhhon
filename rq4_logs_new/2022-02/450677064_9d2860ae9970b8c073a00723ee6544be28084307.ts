import Questionnaire from '@/objects/Questionnaire';
import { Component, OnInit, ViewChild } from '@angular/core';
import { QuestionnaireService } from '@services/serviceQuestionnaire/questionnaire.service';
import { Observable, Subject } from 'rxjs';
import { takeUntil } from 'rxjs/operators';

@Component({
  selector: 'app-choix-thematique',
  templateUrl: './choix-thematique.component.html',
  styleUrls: ['./choix-thematique.component.scss']
})
export class ChoixThematiqueComponent implements OnInit {
  @ViewChild('errorModal') errorModal: any;
  private _questionnaires: Observable<Questionnaire[]>;

  constructor( private questionnaireService : QuestionnaireService) {}

  ngOnInit(): void {
    this._questionnaires = this.getAll();
  }

  getAll(): Observable<Questionnaire[]>{
    let finalise = new Subject();
    let obs = this.questionnaireService.getAll();
    obs.pipe(takeUntil(finalise)).subscribe((data) =>{
        //console.log(data)
        finalise.next();
        finalise.complete();
      },
      (err) => {
        this.errorModal.open(JSON.stringify(err.error));
        finalise.next();
        finalise.complete();
      });
    return obs;
  }


  get questionnaires() : Observable<Questionnaire[]>{
    return this._questionnaires;
  }
}