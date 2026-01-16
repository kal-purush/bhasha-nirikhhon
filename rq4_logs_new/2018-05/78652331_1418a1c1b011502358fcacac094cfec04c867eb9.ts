import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import { catchError, map } from 'rxjs/operators';

import { Config } from '../../../app.config';
import { City, Country } from '../../models';
import { ShortUserInfo } from '../../short-user-info/short-user-info.interface';
import { ArtistType, FanType } from '../../../enums/user-types.enum';
import { parseDateAccordingToTimeZone } from '../../../assets/functions/parse-date-according-totime-zone';

export interface NewTipRqstObj {
  addresser: string;
  addressee: string;
  date: Date;
  amountTip: string;
}

export interface GetTipsQuery {
  findByAddressee: string;
}

interface TipsResponse {
  addresser: {
    avatar: string;
    username: string;
    type: string;
    location: {
      city: City,
      country: Country
    };
  };
  date: Date;
  amountTip: string;
}

@Injectable()
export class TipsManagementService {
  constructor(private http: HttpClient) {
  }

  getTips(query: GetTipsQuery): Observable<ShortUserInfo[]> {
    const queryStr = Config.objToQuery(query);

    return this.http.get(`${Config.api}/get-tips?${queryStr}`)
      .pipe(
        map((tips: TipsResponse[]) => tips.map(tip => this.transformTipToShortUserInfo(tip))),
        catchError(err => {
        console.error('something went wrong: ', err);

        return Observable.throw(err.error)
      }));
  }

  saveTip(tip: NewTipRqstObj): Observable<ShortUserInfo> {
    return this.http.post(`${Config.api}/save-tip`, JSON.stringify(tip), Config.httpOptions)
      .pipe(
        map((savedTip: TipsResponse) => this.transformTipToShortUserInfo(savedTip)),
        catchError(err => {
        console.error('something went wrong: ', err);

        return Observable.throw(err.error)
      }));
  }

  transformTipToShortUserInfo(tip: TipsResponse): ShortUserInfo {
    return {
      avatar: tip.addresser.avatar,
      username: tip.addresser.username,
      location: tip.addresser.location.country.name,
      type: tip.addresser.type === ArtistType.type ? ArtistType.title : FanType.title,
      date: parseDateAccordingToTimeZone({date: tip.date, dateFormat: 'dddd, MMMM DD YYYY'}),
      amountTip: tip.amountTip
    }
  }
}