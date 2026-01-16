import {ChangeDetectionStrategy, Component, EventEmitter, Input, Output} from '@angular/core';
import HelpRequest from '../../../../../shared/model/help-request.model';
import User from '../../../../../shared/model/user.model';

@Component({
  selector: 'app-smart-help-request-detail-action-bar',
  template: `
      <div class="container text-center mt-3 mb-3" *ngIf="isParticipant(); else participantContainer">
          <div class="row pt-2 centered">
              <div class="col-6">
                  <button class="btn btn-lg btn-primary" (click)="volunteer.emit()">Ofera-te ca voluntar</button>
              </div>
          </div>
      </div>
      <ng-template #participantContainer>
          <div class="container text-center mt-3 mb-3">
              <div class="row pt-2 centered">
                  <button class="col-6 btn btn-lg btn-danger" (click)="removeParticipation.emit()">Retrage-te din lista
                      de
                      voluntari
                  </button>
              </div>
              <div class="row pt-2 centered">
                  <button class="col-6 btn btn-lg btn-success" (click)="markAsDone.emit(helpRequest.id)">Marcheaza fapta
                      ca
                      rezolvata
                  </button>
              </div>
          </div>
      </ng-template>
  `,
  styleUrls: ['./smart-help-request-detail-action-bar.component.scss'],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class SmartHelpRequestDetailActionBarComponent {
  @Input() helpRequest: HelpRequest;
  @Input() currentUser: User;
  @Output() volunteer = new EventEmitter<void>();
  @Output() removeParticipation = new EventEmitter<void>();
  @Output() markAsDone: EventEmitter<string> = new EventEmitter<string>();

  isParticipant() {
    return this.helpRequest.volunteers.map(user => user.id).includes(this.currentUser.id);
  }
}