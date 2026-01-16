import {Component, OnInit, ViewChild, AfterViewInit} from "@angular/core";
import {WordPair} from "../../entities/wordpair";
import {User} from "../../entities/user";
import {AlertService} from "../../services/alert.service";
import {AuthService} from "../../services/auth.service";
import {Router} from "@angular/router";
import {WordPairService} from "../../services/wordpair.service";
import {StudyPhaseComponent} from "../studyphase.component";
/**
 * Created by matthewRanftle1 on 7/6/17.
 */

@Component({
  selector: 'memory-task',
  templateUrl: '../../templates/memorytask.component.html',
  providers: [AuthService,
    WordPairService]

})

export class MemoryTaskComponent implements OnInit {
  @ViewChild(StudyPhaseComponent) studyPhaseComponent: StudyPhaseComponent;
  wordPairs: WordPair[];
  jwt: string;
  loaded: boolean;
  userId: number;
  isTest: number;

  constructor(private userService: AuthService,
              private wordPairService: WordPairService) {
  }

  ngOnInit() {
      this.loaded = false;
      this.jwt = this.userService.getJwt();
      this.wordPairService.getWordPairs().then(
        (wordPairs) => {
          this.wordPairs = wordPairs;
          this.wordPairService.getTestOrTrain().then(
            (userInfo) => {
              this.userId = userInfo['id'];
              this.isTest = userInfo['test_phase'];
              this.loaded = true;
            }
          );
        }
      );
  }
}