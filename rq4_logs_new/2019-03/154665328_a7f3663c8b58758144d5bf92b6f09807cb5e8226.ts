import { Injectable } from '@angular/core';

import { AngularFirestore, AngularFirestoreCollection, AngularFirestoreDocument, DocumentChangeAction } from '@angular/fire/firestore';
import { Membership } from 'src/app/teams/membership';
import { Observable } from 'rxjs';

@Injectable()
export class TeamsService {

  membersCollection: AngularFirestoreCollection<Membership>;
  teamsCollection: AngularFirestoreCollection<Team>;

  constructor(private afs: AngularFirestore) {
    this.membersCollection = this.afs.collection('membership');
    this.teamsCollection = this.afs.collection('teams');
  }

  addMembership(uid: string, teamId: string) {
    this.membersCollection.doc<Membership>(uid).set({
      uid: uid,
      isApproved: false,
      isLead: false,
      teamId: teamId
    });
  }
  
  addToTeam(uid: string) {
    this.membersCollection.doc<Membership>(uid).update({
      isApproved: true
    });
  }

  getMembership(uid: string): Observable<Membership> {
    return this.membersCollection.doc<Membership>(uid).valueChanges();
  }

  getTeams(): Observable<Team[]> {
    return this.teamsCollection.snapshotChanges().map(
      (actions) => {
        return actions.map((a) => {
          const data = a.payload.doc.data();
          return { id: a.payload.doc.id, ...data };
        });
      });
  }

  getTeamMembers(teamId: string) {
    return this.afs.collection('membership', ref => ref.where('teamId', '==', teamId).where('isApproved', '==', true)).snapshotChanges();
  }

  getTeamMembersForApproval(teamId: string) {
    return this.afs.collection('membership', ref => ref.where('teamId', '==', teamId).where('isApproved', '==', false)).snapshotChanges();
  }

  getObservableTeamId(uid: string) {
    const membership = this.getMembership(uid);
    const teamId = membership.map( member => {
      return member.teamId;
    });

    return teamId;
  }

  isLead(uid: string): Observable<boolean> {
    return this.membersCollection.doc<Membership>(uid).valueChanges().map(membership => {
      return membership.isLead;
    })
  }

  removeTeamMember(uid: string) {
    this.membersCollection.doc<Membership>(uid).delete();
  }


}