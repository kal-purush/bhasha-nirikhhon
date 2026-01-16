import { Injectable } from '@angular/core';
import { ConfigService } from '../config/config.service';
import { Http, Headers } from '@angular/http';

@Injectable()
export class YelpService {
  constructor(private http: Http, private configService: ConfigService) {}

  get yelpBusinessSearch(): string {
    return this.configService.yelpBusinessSearch;
  }

  // TODO: create interface for options
  getBusinesses(latitude: number, longitude: number, options?: any) {
    const params = {
      latitude,
      longitude
    };

    // TODO: these headers should probably go on our backend
    var headers = new Headers();
    headers.append('Authorization', 'Bearer 6kEyu_lw6RQSRKiRI8ZQocSznf2Zi1AQO7u-bZ0HNF9xjRTsEwTSIeNoolGr9zipiP5-f9NXGwc4m24AdqLlkqMCkpPM7UuJKDbhVqYgm7RBDnpykQBsy822EvCtWXYx');

    return this.http.get(this.yelpBusinessSearch, { params, headers });
  }
}