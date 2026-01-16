import {Directive, ElementRef, Renderer2} from '@angular/core';
import {MessageService} from '../services/directive-messaging';
import {Subscription} from 'rxjs';

@Directive({
  selector: '[appScoreBoard]'
})
export class ScoreBoardDirective {
  private subscription: Subscription;

  constructor(elem: ElementRef, renderer: Renderer2, messageService: MessageService) {
    const ctx = elem.nativeElement.getContext('2d');
    let circleScore = 0;
    let squareScore = 0;
    this.subscription = messageService.subscribe('SquareGetScore', (payload) => {
      squareScore = payload.squareScore;
      updateScore();
    });
    this.subscription = messageService.subscribe('CircleGetScore', (payload) => {
      circleScore = payload.circleScore;
      updateScore();
    });
    const updateScore = () => {
      const totalScore = circleScore + squareScore;
      document.getElementById('scoreBoard').innerHTML = '' + totalScore;
    };
  }
}