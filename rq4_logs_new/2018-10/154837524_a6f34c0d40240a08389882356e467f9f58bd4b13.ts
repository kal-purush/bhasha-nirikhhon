import { Injectable } from '@angular/core';
import {Penalty, Packet, RoleType, SuspectNote, SuspectRole} from './card-definitions';
import {PENALTIES, PACKETS, ROLE_TYPES, SUSPECT_NOTES} from './card-objects';
import {RandomService} from './random.service';

@Injectable({
  providedIn: 'root'
})
export class PacketService {

  constructor(private random: RandomService) { }

  /**
   * Gets three distinct, random suspect roles from a packet
   * @param packet The packet to retrieve roles from
   */
  getSuspectRoles(packet: Packet): SuspectRole[] {
    const roles = packet.roleCards;
    const nums = this.random.getRandomNumbers(0, roles.length, 3);
    return [roles[nums[0]], roles[nums[1]], roles[nums[2]]];
  }

  /**
   * Gets a single random suspect role from a packet,
   * for more experienced players.
   * @param packet The packet to retrieve the role from
   */
  getSuspectRole(packet: Packet): SuspectRole {
    const rand = this.random.getRandomNumber(0, packet.roleCards.length);
    return packet.roleCards[rand];
  }
}