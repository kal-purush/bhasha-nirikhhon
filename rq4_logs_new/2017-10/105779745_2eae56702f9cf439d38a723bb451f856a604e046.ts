import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

import { Activity } from '../../models/activity';

@Injectable()
export class Activities {
  activities: Activity[] = [];

  defaultActivity: any = {
    "name": "上海环球港店",
    "type": "直营店",
    "province": "上海",
    "city": "普陀区",
    "address": "上海市金沙江路001号",
    "profilePic": "assets/img/speakers/bear.jpg",
    "about": "上海环球港店是Gamepoch星游纪的第一个直营店，这个店非常棒，位置很好，客流量很大。",
  };

  constructor(public http: Http) {
    let activities = [
      {
        "name": "上海环球港店",
        "type": "直营店",
        "province": "上海",
        "city": "普陀区",
        "address": "上海市金沙江路001号",
        "profilePic": "assets/img/speakers/bear.jpg",
        "about": "上海环球港店是Gamepoch星游纪的第一个直营店，这个店非常棒，位置很好，客流量很大。",
      },
      {
        "name": "上海外滩店",
        "type": "一级门店",
        "province": "上海",
        "city": "黄浦区",
        "address": "上海市金黄浦区沙江路001号",
        "profilePic": "assets/img/speakers/cheetah.jpg",
        "about": "上海环球港店是Gamepoch星游纪的一级门店，在上海著名的外滩旁，风景优美，商业气息浓厚。"
      },
      {
        "name": "浙江义乌店",
        "type": "二级门店",
        "province": "浙江",
        "city": "义乌市",
        "address": "浙江省义乌市XX县xx大街xxx号",
        "profilePic": "assets/img/speakers/duck.jpg",
        "about": "浙江义乌店是Gamepoch星游纪的第一个二级门店，这个店非常棒，位置很好，客流量很大。"
      }
    ];

    for (let activity of activities) {
      this.activities.push(new Activity(activity));
    }
  }

  query(params?: any) {
    if (!params) {
      return this.activities;
    }

    return this.activities.filter((item) => {
      for (let key in params) {
        let field = item[key];
        if (typeof field == 'string' && field.toLowerCase().indexOf(params[key].toLowerCase()) >= 0) {
          return item;
        } else if (field == params[key]) {
          return item;
        }
      }
      return null;
    });
  }

  add(activity: Activity) {
    this.activities.push(activity);
  }

  delete(activity: Activity) {
    this.activities.splice(this.activities.indexOf(activity), 1);
  }
}