import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class UserserviceService {

  constructor() { }

  getAllGroups() {
    return [
      {
        supportId: 1,
        supportName: 'Alcohol for days',
        userList: ['Jason', 'Jyothi', 'Easley'] ,
        addiction: 'Alcohol',
      },
      {
        supportId: 2,
        supportName: 'Cocaine for days',
        userList: ['Jack', 'Henry', 'Katie'] ,
        addiction: 'Cocaine',
      },
      {
        supportId: 3,
        supportName: 'Gambling for days',
        userList: ['Water', 'Pepsi', 'Jason'] ,
        addiction: 'Gambling',
      },
      {
        supportId: 4,
        supportName: 'Meth for days',
        userList: ['Jason', 'Pen', 'Snickers'] ,
        addiction: 'Meth',
      },
      {
        supportId: 5,
        supportName: 'Pain Killers for days',
        userList: ['Starbucks', 'Panera', 'Easley'] ,
        addiction: 'Pain Killers',
      },
      {
        supportId: 6,
        supportName: 'Nicotine for days',
        userList: ['Sprite', 'Paper', 'Jason'] ,
        addiction: 'Nicotine',
      },
      {
        supportId: 7,
        supportName: 'Heroine for days',
        userList: ['Sprite', 'Paper', 'Jason'] ,
        addiction: 'Heroine',
      },
      {
        supportId: 8,
        supportName: 'wallstreetbets addiction help zone',
        userList: ['Onions', 'Pork Chops', 'Jason'] ,
        addiction: 'Gambling',
      },
      {
        supportId: 9,
        supportName: 'Carrots against drinking',
        userList: ['Merkat', 'Boolean', 'Saol'] ,
        addiction: 'Alcohol',
      },
      {
        supportId: 10,
        supportName: 'SPY 0 days recovery post',
        userList: ['Wizzdaddy', 'Bloombindale', 'Jason'] ,
        addiction: 'Gambling',
      },
    ];
  }

  getMyGroups() {
    return [
      {
        supportId: 1,
        supportName: 'Alcohol for days',
        userList: ['Jason', 'Jyothi', 'Easley'],
        postList: [{postId: 1, postBody: 'Im so sad because of this....',
        userId: 1}, {postId: 2, postBody: 'hey does this work', userId: 1}],
        replyList: [{replyId: 1, replyBody: 'This is my reply to sadness :(', postId: 1, userId: 2},
        {replyId: 2, replyBody: 'Keep on drinking!', postId: 1, userId: 1}],
        addiction: 'alcohol',
      },
      {
        supportId: 2,
        supportName: 'Cocaine for days',
        userList: ['Jack', 'Henry', 'Katie'] ,
        postList: [{postId: 5, postBody: 'Alcohol is my gateway to freedom...', userId: 2},
        {postId: 4, postBody: 'Cocaine is not good', userId: 2}],
        replyList: [{replyId: 3, replyBody: 'YOLO!', postId: 5, userId: 2}, {replyId: 4,
        replyBody: 'Just keep swimming, Just keep swimming', postId: 5, userId: 1}],
        addiction: 'Cocaine',
      },
      {
        supportId: 3,
        supportName: 'Gambling for days',
        userList: ['Water', 'Pepsi', 'Jason'] ,
        postList: [],
        replyList: [],
        addiction: 'Gambling',
      },
      {
        supportId: 8,
        supportName: 'wallstreet addiction help zone',
        userList: ['Onions', 'Pork Chops', 'Jason'] ,
        postList: [],
        replyList: [],
        addiction: 'Gambling',
      },
      {
        supportId: 10,
        supportName: 'SPY 0 days recovery post',
        userList: ['Wizzdaddy', 'Bloombindale', 'Jason'] ,
        postList: [],
        replyList: [],
        addiction: 'Gambling',
      },
    ];
  }


}