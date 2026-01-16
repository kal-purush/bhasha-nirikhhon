/**
 * Created by matthewRanftle1 on 7/11/17.
 */
import {Component, OnInit, ViewChild, Input} from '@angular/core';
import { WordPair } from '../entities/wordpair';
import { WordPairService } from '../services/wordpair.service';
import Timer = NodeJS.Timer;
import {Router, ActivatedRoute} from "@angular/router";

@Component({
  selector: 'test-phase-feed-back',
  templateUrl: '../templates/testphasefeedback.component.html',
  styleUrls:['../stylesheets/testphase.component.css'],
  providers: [WordPairService]
})

export class TestPhaseFeedbackComponent implements OnInit {
  // instructions = "Instructions here";
  @ViewChild('response') response: any;
  @Input() wordPairs: WordPair[];
  i: number;
  selectedWordPair: WordPair;
  showCorrect: boolean;
  responseTime: number;
  timer: Timer;
  @Input() testPhase: number;
  sub: any;

  constructor( private wordPairService: WordPairService,
               private router: Router,
               private route: ActivatedRoute) {}

  submitPair(): void {
    var elipsedTime = Date.now() - this.responseTime;
    var ur = this.response.nativeElement.value || null;
    this.response.nativeElement.value = null;
    var userresponse = {
      word1: this.wordPairs[this.i].word1,
      word2: this.wordPairs[this.i].word2,
      response_number:this.i,
      response:  ur,
      response_time: elipsedTime,
      test_phase: this.testPhase

    };
    console.log(userresponse);
    this.wordPairService.saveUserResponse(userresponse);
    this.showCorrect = true;

    // FEEDBACK
    setTimeout(() => {
      if (this.i < this.wordPairs.length - 1) {
        this.i++;
      } else {

        //testing done, route to next component
        clearTimeout(this.timer);
        this.testPhase++;
        return;
      }
      this.showCorrect = false;
      this.resetTimer();

    }, 1000); // FEEDBACK TIME
  }

  resetTimer() : void {
    clearTimeout(this.timer);
    this.timer = setTimeout(() => this.submitPair(), 5000); // TESTING TIME
    this.responseTime = Date.now();
  }

  ngOnInit(): void {
    this.i = 0;
    this.responseTime = Date.now();
    this.timer = setTimeout(() => this.submitPair(), 5000); // TESTING TIME
  }
}