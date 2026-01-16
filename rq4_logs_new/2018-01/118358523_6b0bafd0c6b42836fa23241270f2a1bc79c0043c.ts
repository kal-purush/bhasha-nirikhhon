import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import { statistic } from './../models/statistic';

@Injectable()
export class StatisticsClientService {
  private baseURL = 'https://project-9-20-187720.appspot.com/';
  constructor(private http: HttpClient) { }

  public getDailyRequestedStatistic(): Observable<Object> {
    const url = this.baseURL +  'statistics/daily/resources/requested';
    return this.http.get(url)
  }

  public getDailyAvailableStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/daily/resources/available';
    return this.http.get(url).map( res => {
      return res['Resource_Available'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }

  public getDailyCompareStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/daily/resources/between_requested_available';
    return this.http.get(url).map( res => {
      return res['Resource_Compare'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }


  public getWeekRequestedStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/trending/resources/requested';
    return this.http.get(url).map( res => {
      return res['Resource_Requested'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }

  public getWeekAvailableStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/trending/resources/available';
    return this.http.get(url).map( res => {
      return res['Resource_Available'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }

  public getWeekCompareStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/trending/resources/between_requested_available';
    return this.http.get(url).map( res => {
      return res['Resource_Compare'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }

  public getMonthRequestedStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/month/resources/requested';
    return this.http.get(url).map( res => {
      return res['Resource_Requested'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }

  public getMonthAvailableStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/month/resources/available';
    return this.http.get(url).map( res => {
      return res['Resource_Available'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }

  public getMonthCompareStatistic(): Observable<statistic[]> {
    const url = this.baseURL +  '/statistics/month/resources/between_requested_available';
    return this.http.get(url).map( res => {
      return res['Resource_Compare'].map( item => {
        return new statistic(
          item.Region,
          item.Amount          
        );
      });
    });
  }



}