import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {UserService} from './user.service';
import {ConnectionService} from './connection.service';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class KnowledgeService {
  private serverAddress: string;

  constructor(private connectionService: ConnectionService, private http: HttpClient, private userService: UserService) {
    this.serverAddress = connectionService.getServerAddress();
  }

  getAllKnowledge(): Observable<Knowledge[]> {
    let allKnowledgeAddress = this.serverAddress + 'allKnowledge';

    return this.http.get<Knowledge[]>(allKnowledgeAddress, {headers: this.userService.getAuthHeaders()});
  }
}