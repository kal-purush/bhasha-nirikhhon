import {Component, EventEmitter, Input, Output} from '@angular/core';
import {User} from "../../../../models/user.model";

@Component({
  selector: 'friend-block',
  templateUrl: './friendBlk.html',
  styles: [`
    .card > .card-body {
      background-color: #efefef;
    }
    .card > .card-header,
    .card > .card-body {
      padding: .3em;
    }
    button.btn.btn-link.friend-name {
      color: #000;
      font-weight: bolder;
      font-size: 24px;
    }
  `]
})

export class FriendBlkComponent {
  @Input() friend: User;
  @Input() currentUser: User;
  @Output() befriend = new EventEmitter<User>();
  @Output() unFriend = new EventEmitter<User>();
  @Output() viewUserProfile = new EventEmitter<User>();
  isFriend() {
    return this.currentUser && this.currentUser.friends && this.currentUser.friends.some(friend => friend.id === this.friend._id);
  }
  toggleFriendship() {
    if (this.isFriend()) {
      this.unFriend.emit(this.friend);
    } else {
      this.befriend.emit(this.friend);
    }
  }
  viewFriendProfile() {
    this.viewUserProfile.emit(this.friend);
  }
  getName(): string {
    let name = '';
    if (this.friend) {
      if (this.friend.firstName && this.friend.lastName) {
        name = `${this.friend.firstName} ${this.friend.lastName}`;
      } else {
        name = this.friend.username;
      }
    } else {
      return name = 'loading...';
    }
    return name;
  }
}