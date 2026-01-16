import { Component, OnInit, Input } from '@angular/core';
import { MatDialog } from '@angular/material';
import { empty, Observable } from 'rxjs';
import { flatMap } from 'rxjs/operators';

import { PlayerQuestService } from '../player-quest/player-quest.service';
import { EmailService } from '../../core/email.service';
import { NotifyService } from '../../core/notify.service';
import { SeasonService } from '../../core/season.service';

import { SubmitQuestDialogComponent } from './submit-quest-dialog.component';

@Component({
  selector: 'player-quest-list',
  templateUrl: './player-quest-list.component.html',
  styleUrls: ['./player-quest-list.component.scss']
})
export class PlayerQuestListComponent implements OnInit {
  readonly displayedColumns = [
    'type',
    'category',
    'questName',
    'status',
    'completed',
    'xp'
  ];
  @Input() user: User;
  public questList: Observable<PlayerQuest[]>;
  constructor(
    private emailService: EmailService,
    private dialog: MatDialog,
    private notifyService: NotifyService,
    private playerQuestService: PlayerQuestService,
    private season: SeasonService) {}

  async ngOnInit(){
    const seasonId = await this.season.getEnabledSeasonId().toPromise();
    if(this.user && this.user.team) {
      const teamId = this.user.team.id;
      this.questList = this.playerQuestService.getMemberQuests(seasonId, teamId, this.user.uid);
    }
  }

  public openSubmitQuestDialog(playerQuest: PlayerQuest) {
    const submitQuestDialog = this.dialog.open(SubmitQuestDialogComponent, { data: playerQuest, width: '600px'});
    submitQuestDialog.afterClosed().subscribe( data => {
      if(data && data.questId) {
        this.playerQuestService.submitQuest(data.questId, data.completed, data.completionProof)
          .then(()=>{
            this.emailService.sendEmail(playerQuest.teamLeadEmail,
              'Player Quest Submitted',
              `Player ${playerQuest.playerName} has completed the quest '${playerQuest.questName}'. Kindly validate the quest completion.`
            );
            this.notifyService.update(`Your quest has been submitted to your team lead for validation.`, 'success');
          })
          .catch((err) => {
            console.log(err);
            this.notifyService.update(`Something went wrong. Please try again later.`, 'error');
          });
      }
    }); 
  }
}