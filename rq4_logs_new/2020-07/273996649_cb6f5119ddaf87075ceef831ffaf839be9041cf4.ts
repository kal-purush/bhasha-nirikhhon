import { Injectable } from '@angular/core';
import { Channel } from '../models/channel';
import { BehaviorSubject } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ChannelService {

  private collection = new BehaviorSubject(null);

  constructor() {
    var mockup = [
      {
        id: 1,
        title: "Channel 1",
        description: "Channel 1 from Workspace 1",
        admin_flag: false,
        people: []
      },
      {
        id: 2,
        title: "Channel 2",
        description: "Channel 2 from Workspace 2",
        admin_flag: false,
        people: []
      },
      {
        id: 3,
        title: "Channel 3",
        description: "Channel 3 from Workspace 3",
        admin_flag: false,
        people: []
      },
    ]
    this.collection.next(mockup);
  }

  public all(): Array<Channel> {
    return this.collection.getValue();
  }
}