import { Component, Input } from '@angular/core';
import { QuestionInterface } from '../../providers/component-dynamic/QuestionInterface';

/**
 * Generated class for the VocaQ1ContainerComponent component.
 *
 * See https://angular.io/docs/ts/latest/api/core/index/ComponentMetadata-class.html
 * for more info on Angular Components.
 */
@Component({
  selector: 'voca-q1-container',
  templateUrl: 'voca-q1-container.html'
})
export class VocaQ1ContainerComponent implements QuestionInterface{
  
  @Input() data: Object;  
  text: string;

  constructor() {
    console.log('Hello VocaQ1ContainerComponent Component');
    this.text = 'Hello World';
  }

  ionViewDidLoad() {
    console.log('ionViewDidLoad VocaQ1ContainerComponent');
  }
}