import { R3TargetBinder } from '@angular/compiler';
import { Injectable } from '@angular/core';
import { CanDeactivate } from '@angular/router';
import { EvaluationQuestionnaireComponent } from '@pages/visitor/evaluer/evaluation-questionnaire/evaluation-questionnaire.component';
@Injectable({
  providedIn: 'root'
})
export class ConfirmDeactivateGuard implements CanDeactivate<EvaluationQuestionnaireComponent> {

    canDeactivate(target: EvaluationQuestionnaireComponent) {
      return window.confirm("Vous avez commencé une évaluation, si vous quittez la page votre saisie sera perdue et l'évaluation abandonnée");

    }

}