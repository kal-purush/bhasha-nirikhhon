import {
  Component,
  OnInit,
  ViewChild,
} from '@angular/core';

import { TitleAppService } from '../../../../shared/module/title-app';
import { RuleApiService } from '../../../../shared/service/rule-api';
import { NotificationComponent } from '../../../../shared/module/notification';

@Component({
  selector: 'app-rule',
  templateUrl: './rule.component.html',
  styleUrls: ['./rule.component.scss']
})
export class RuleComponent implements OnInit {

  @ViewChild('notification') notification: NotificationComponent;
  public rulePlayer;
  public ruleGoals;
  public rulePoint;

  constructor(private ruleApiService: RuleApiService,
              private titleAppService: TitleAppService) {
  }

  public ngOnInit() {
    this.titleAppService.setTitle('Quy định giải đấu');
    this.getRule();
  }

  public onSavePlayer(value) {
    const {player} = value;
    this.ruleApiService.updateRulePlayer(player)
      .subscribe((response: any) => {
        if (response.result) {
          this.getRule();
          this.notification
            .onSuccess('Đã cập nhật quy định về cầu thủ', 'Thành công');
        }
      });
  }

  public onSaveGoal(value) {
    this.ruleApiService.updateRuleGoal(value)
      .subscribe((response: any) => {
        if (response.result) {
          this.getRule();
          this.notification
            .onSuccess('Đã cập nhật quy định về bàn thắng', 'Thành công');
        }
      });
  }

  public onSavePoint(value) {
    this.ruleApiService.updateRulePoint(value)
      .subscribe((response: any) => {
        if (response.result) {
          this.getRule();
          this.notification
            .onSuccess('Đã cập nhật quy định về điểm số', 'Thành công');
        }
      });
  }

  public getRule() {
    this.ruleApiService.getRule()
      .subscribe((response: any) => {
        if (response.result) {
          const {player, goals, point} = response.data[0];
          this.rulePlayer = player;
          this.ruleGoals = goals;
          this.rulePoint = point;
          console.log(response);
        }
      });
  }

}

