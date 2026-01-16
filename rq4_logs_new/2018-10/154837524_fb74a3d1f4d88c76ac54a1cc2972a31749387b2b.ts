import { Injectable } from '@angular/core';
import {Penalty, Packet, RoleType, SuspectNote} from './card-definitions';
import {PENALTIES, PACKETS, ROLE_TYPES, SUSPECT_NOTES} from './card-objects'
import {RandomService} from './random.service';

@Injectable({
  providedIn: 'root'
})
export class CardService {

  constructor(private random: RandomService) { }

  getPenalties() : Penalty[] {
    let nums = this.random.getRandomNumbers(0, PENALTIES.length, 3);
    return [PENALTIES[nums[0]], PENALTIES[nums[1]], PENALTIES[nums[2]]];
  }

  getPenalty(id: number) : Penalty{
    let result = null;
    PENALTIES.forEach((penalty) => {
      if(penalty.id === id){
        result = penalty;
      }
    });
    return result;
  }
}