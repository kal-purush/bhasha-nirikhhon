import { Component, OnInit, Input } from '@angular/core';
import { MatTableDataSource } from '@angular/material';
import { TeamsService } from 'src/app/teams/teams.service';
import { AuthService } from 'src/app/core/auth.service';
import { DocumentChangeAction } from '@angular/fire/firestore';

@Component({
  selector: 'team-applications',
  templateUrl: './team-applications.component.html',
  styleUrls: ['./team-applications.component.scss']
})
export class TeamApplicationsComponent implements OnInit {

  @Input() currentUser: User;
  teamApplicationDataSource: MatTableDataSource<Membership>;
  teamApplications: Membership[];

  applicationColumns = ['displayName', 'acceptButton', 'kickButton'];

  constructor(public auth: AuthService,
    private teamsService: TeamsService) { }

  ngOnInit() {
    if (!this.currentUser) {
      this.auth.user.subscribe((user: User) => {
        if (!user) {
          return;
        }
        this.currentUser = user;
        this.init();
      });
    } else {
      this.init();
    }
  }

  private init() {
    this.teamApplicationDataSource = new MatTableDataSource<Membership>(this.teamApplications);
    this.teamsService.getTeamId(this.currentUser.uid).subscribe(teamId => {
      this.teamsService.getTeamMembersForApproval(teamId).subscribe((arr: DocumentChangeAction<Membership>[]) => {
        this.teamApplications = this.mapMembershipDocument(arr);
        this.teamApplicationDataSource = new MatTableDataSource<Membership>(this.teamApplications);
      });
    });
  }

  private mapMembershipDocument(arr: DocumentChangeAction<Membership>[]) {
    return arr.map(item => {
      const data = item.payload.doc.data();
      return {
        ...data
      } as Membership;
    });
  }

}