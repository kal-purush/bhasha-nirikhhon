import {Event} from '../event';
import {ActivityCategory} from '../activity-category'


export const DUMMY_DATA_EVENTS: Event[] = [
  {"EventId":1,"FromDate":new Date("2017-10-18T08:30:00"),"ToDate":new Date("2017-10-18T10:30:00"),"EnteredByUsername":"b","ActivityCategoryId":3,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":2,"FromDate":new Date("2017-10-20T17:30:00"),"ToDate":new Date("2017-10-20T19:15:00"),"EnteredByUsername":"a","ActivityCategoryId":3,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":3,"FromDate":new Date("2017-10-20T19:00:00"),"ToDate":new Date("2017-10-20T20:00:00"),"EnteredByUsername":"a","ActivityCategoryId":4,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":4,"FromDate":new Date("2017-10-21T08:30:00"),"ToDate":new Date("2017-10-21T10:30:00"),"EnteredByUsername":"a","ActivityCategoryId":5,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":5,"FromDate":new Date("2017-10-21T10:30:00"),"ToDate":new Date("2017-10-21T12:00:00"),"EnteredByUsername":"a","ActivityCategoryId":6,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":6,"FromDate":new Date("2017-10-22T08:30:00"),"ToDate":new Date("2017-10-22T10:30:00"),"EnteredByUsername":"a","ActivityCategoryId":9,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":7,"FromDate":new Date("2017-10-22T07:30:00"),"ToDate":new Date("2017-10-22T08:30:00"),"EnteredByUsername":"a","ActivityCategoryId":8,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":8,"FromDate":new Date("2017-10-22T08:30:00"),"ToDate":new Date("2017-10-22T10:30:00"),"EnteredByUsername":"a","ActivityCategoryId":10,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":9,"FromDate":new Date("2017-10-22T10:30:00"),"ToDate":new Date("2017-10-22T12:00:00"),"EnteredByUsername":"a","ActivityCategoryId":11,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
  {"EventId":10,"FromDate":new Date("2017-10-17T08:30:00"),"ToDate":new Date("2017-10-17T10:30:00"),"EnteredByUsername":"a","ActivityCategoryId":1,"ActivityCategory":null,"CreationDate":new Date("2017-09-17T10:30:00"),"IsActive":true},
]


export const DUMMY_DATA_ACTIVITY_CATEGORIES: ActivityCategory[] = [
  {"ActivityCategoryId":1,"ActivityDescription":"Senior's Golf Tournament","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":2,"ActivityDescription":"Leadership General Assembly Meeting","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":3,"ActivityDescription":"Youth Bowling Tournament","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":4,"ActivityDescription":"Young Ladies Cooking Lessons","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":5,"ActivityDescription":"Youth Craft Lessons","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":6,"ActivityDescription":"Youth Choir Practice","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":7,"ActivityDescription":"Lunch","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":8,"ActivityDescription":"Pancake Breakfast","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":9,"ActivityDescription":"Swimming Lessons for Youth","CreationDate":new Date("2017-09-17T10:30:00")},
  {"ActivityCategoryId":10,"ActivityDescription":"Swimming Exercise for Parents","CreationDate":new Date("2017-09-17T10:30:00")},
]