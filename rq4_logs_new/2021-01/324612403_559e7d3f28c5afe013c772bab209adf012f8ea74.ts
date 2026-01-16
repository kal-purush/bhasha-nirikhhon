import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
// import {Video} from '../../mock-videos';
import {API_URL} from '../../app.constants';



@Injectable({
  providedIn: 'root'
})
export class VideodataService {

  constructor(private http: HttpClient) { }

  // retrieveAllVideos = () => this.http.get<Video[]>(
  //   `${API_URL}/users/videos`
  // )
  // retrieveUserTodo = (username: any) => this.http.get<Video[]>(
  //   `${API_URL}/users/${username}/videos`
  // )
  // deleteVideo = (username: any, id: any) => this.http.delete(
  //   `${API_URL}/users/${username}/videos/${id}`
  // )
  // updateVideo = (username: any, id: any, video) => this.http.put<Video>(
  //   `${API_URL}/users/${username}/videos/${id}`, video
  // )
  // uploadVideo = (username: any, video) => this.http.post<Video>(
  //   `${API_URL}/users/${username}/videos/`, video
  // )
}