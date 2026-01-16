import { Injectable } from '@angular/core';
import { ApiService } from '../api/api.service';
import { RoomAccount, PageRequest, PageData } from '@gmrc/models';

@Injectable({
  providedIn: 'root'
})
export class RoomAccountService {

  constructor(private apiService: ApiService) { }
  addRoomAccount(body: RoomAccount) {
    return this.apiService.post<RoomAccount>('roomAccount/', body);
   }
   updateRoomAccount(body: RoomAccount) {
     return this.apiService.put<RoomAccount>('roomAccount/', body);
   }
   getRoomAccounts<T>(pageRequest: PageRequest) {
     return this.apiService.post<PageData<T>>('roomAccount/page', pageRequest);
   }
}