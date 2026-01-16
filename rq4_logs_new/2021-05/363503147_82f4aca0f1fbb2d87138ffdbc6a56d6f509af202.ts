import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Channel } from './channel';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ChannelService {
  private channelUrls: string;

  constructor(private http: HttpClient) {
    this.channelUrls = 'http://localhost:8080/Channel';
  }

  public findAll(): Observable<Channel[]> {
    return this.http.get<Channel[]>(this.channelUrls);
  }

  public findById(id: number): Observable<Channel> {
    return this.http.get<Channel>(this.channelUrls + `/${id}`);
  }

  public createChannel(channel: Channel): Observable<Channel> {
    return this.http.post<Channel>(this.channelUrls, channel);
  }

  public updateChannel(id: number, channel: Channel): Observable<Channel> {
    return this.http.put<Channel>(this.channelUrls + `/${id}`, channel);
  }

  public deleteChannel(id: number): Observable<boolean> {
    return this.http.delete<boolean>(this.channelUrls + `/${id}`);
  }

}