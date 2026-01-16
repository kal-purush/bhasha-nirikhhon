import { Injectable } from '@angular/core';
import { SupportGroup } from 'src/app/models/supportgroup';
import { User } from 'src/app/models/user';

@Injectable({
  providedIn: 'root'
})
export class GroupserviceService {
  
  users: User[] = [
    {
      userId: 1,
      username: 'jasonkim4201',
      password: 'password123',
      email: 'jasonkim4201@gmail.com'
    },
    {
      userId: 2,
      username: 'jyothit',
      password: 'password1234',
      email: 'jyothit@gmail.com'
    },
    {
      userId: 3,
      username: 'eboo1234',
      password: 'password12345',
      email: 'eboo@gmail.com'
    }
  ];


  myarr = [];
  tempGroup: SupportGroup = {
    supportName: '',
    addiction: null,
    userList: null
  };

  mySupportGroup: SupportGroup[] = [
    {
      supportId: 1,
      supportName: 'Alcohol for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      postList: [{
        postId: 1, postBody: 'Im so sad because of this....',
        userId: 1
      }, { postId: 2, postBody: 'hey does this work', userId: 1 }],
      replyList: [{ replyId: 1, replyBody: 'This is my reply to sadness :(', postId: 1, userId: 2 },
      { replyId: 2, replyBody: 'Keep on drinking!', postId: 1, userId: 1 }],
      addiction: '1',
    },
    {
      supportId: 2,
      supportName: 'Cocaine for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      postList: [{ postId: 5, postBody: 'Alcohol is my gateway to freedom...', userId: 2 },
      { postId: 4, postBody: 'Cocaine is not good', userId: 2 }],
      replyList: [{ replyId: 3, replyBody: 'YOLO!', postId: 5, userId: 2 }, {
        replyId: 4,
        replyBody: 'Just keep swimming, Just keep swimming', postId: 5, userId: 1
      }],
      addiction: '7',
    },
    {
      supportId: 3,
      supportName: 'Gambling for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      postList: [],
      replyList: [],
      addiction: '5',
    },
    {
      supportId: 8,
      supportName: 'wallstreet addiction help zone',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      postList: [],
      replyList: [],
      addiction: '7',
    },
    {
      supportId: 10,
      supportName: 'SPY 0 days recovery post',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      postList: [],
      replyList: [],
      addiction: '7',
    },
  ];



  allGroups: SupportGroup[] = [
    {
      supportId: 1,
      supportName: 'Alcohol for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Alcohol',
    },
    {
      supportId: 2,
      supportName: 'Cocaine for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Cocaine',
    },
    {
      supportId: 3,
      supportName: 'Gambling for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Gambling',
    },
    {
      supportId: 4,
      supportName: 'Meth for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Meth',
    },
    {
      supportId: 5,
      supportName: 'Pain Killers for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Pain Killers',
    },
    {
      supportId: 6,
      supportName: 'Nicotine for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Nicotine',
    },
    {
      supportId: 7,
      supportName: 'Heroine for days',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Heroine',
    },
    {
      supportId: 8,
      supportName: 'wallstreetbets addiction help zone',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Gambling',
    },
    {
      supportId: 9,
      supportName: 'Carrots against drinking',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Alcohol',
    },
    {
      supportId: 10,
      supportName: 'SPY 0 days recovery post',
      userList: [
        {
          userId: 1,
          username: 'jasonkim4201',
          password: 'password123',
          email: 'jasonkim4201@gmail.com'
        },
        {
          userId: 2,
          username: 'jyothit',
          password: 'password1234',
          email: 'jyothit@gmail.com'
        },
        {
          userId: 3,
          username: 'eboo1234',
          password: 'password12345',
          email: 'eboo@gmail.com'
        }
      ],
      addiction: 'Gambling',
    },

  ];


  constructor() { }



  getMyGroups(): SupportGroup[] {
    return this.mySupportGroup;
  }

  getAllGroups(): SupportGroup[] {
    return this.allGroups;
  }
  myNewGroup(newGroup: SupportGroup): SupportGroup {
    console.log(this.users);
    this.mySupportGroup.push(newGroup);
    console.log('This is my new Support Group' + this.mySupportGroup);

    this.myarr.push(newGroup.supportName, newGroup.addiction);
    console.log(this.myarr);

    this.tempGroup = newGroup;

    console.log('This is my tempGroup');
    console.log(this.tempGroup);
    return this.tempGroup;


  }



}