import * as moment from 'moment';

import { LaunchEvent } from './event-launch.interface';

export class Show {
  _id?: string;
  name: string;
  creator: string;
  description: string;
  artist: string;
  info: string;
  genres: string[];
  posters: string[];
  audios: string[];
  videos: string[];
  appreciations: string[];
  live: boolean;
  completed: boolean;
  location: {
    country: string;
  };
  dateCreated: number;
  datePerformance: number;
  timePerformance: {
    start: string;
    end: string;
  };
  tickets: {
    count: number;
    ticketPrice: number;
    ticketsToFund: number;
    ticketsSold: number;
    fundedPercentage: number;
  };
  statistics: {
    likes: string[];
    viewers: string[];
    followers: string[];
  };
  wowza: {
    id: string;
    [key: string]: any
  };

  constructor(eventData?: LaunchEvent) {
    this._id = eventData ? eventData._id : null;
    this.name = eventData ? eventData.name : '';
    this.creator = eventData ? eventData.creator : '';
    this.description = eventData ? eventData.description : '';
    this.artist = eventData ? eventData.artist : '';
    this.info = eventData ? eventData.info : '';
    this.genres = eventData ? eventData.genres : [];
    this.posters = eventData ? eventData.posters : [];
    this.audios = eventData ? eventData.audios : [];
    this.videos = eventData ? eventData.videos : [];
    this.appreciations = eventData ? eventData.appreciations : [];
    this.live = eventData ? eventData.live : false;
    this.completed = eventData ? eventData.completed : false;
    this.location = eventData ? eventData.location : {country: ''};
    this.dateCreated = eventData ? eventData.dateCreated : new Date().getTime();
    this.datePerformance = eventData ? eventData.datePerformance : new Date().getTime();
    this.timePerformance = eventData ? eventData.timePerformance : {
      start: moment(new Date()).format('dddd, MMMM DD YYYY, h:mm:ss a'),
      end: moment(new Date()).format('dddd, MMMM DD YYYY, h:mm:ss a')
    };
    this.tickets = eventData ? eventData.tickets : {
      count: 0,
      ticketPrice: 0,
      ticketsToFund: 0,
      ticketsSold: 0,
      fundedPercentage: 0,
    };
    this.statistics = eventData ? eventData.statistics : {
      likes: [],
      viewers: [],
      followers: []
    };
    this.wowza = eventData ? eventData.wowza : {id: null};
  }
}