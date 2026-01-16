import { Component, Input } from '@angular/core';
import { QuestionInterface } from '../../providers/component-dynamic/QuestionInterface';

/**
 * Generated class for the VocaExamComponent component.
 *
 * See https://angular.io/docs/ts/latest/api/core/index/ComponentMetadata-class.html
 * for more info on Angular Components.
 */
@Component({
  selector: 'voca-exam',
  templateUrl: 'voca-exam.html'
})
export class VocaExamComponent implements QuestionInterface {
  @Input() data: Object;

  constructor() {
    console.log('Hello VocaExamComponent Component');
  }

}