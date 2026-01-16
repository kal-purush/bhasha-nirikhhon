import { Injectable } from '@angular/core';
import { Player, RoomService } from './room/room.service';
import { PlayerStatusService } from './player-status.service';
import { take } from 'rxjs/operators';

@Injectable({
    providedIn: 'root'
})
export class PlayerSessionService {
    constructor (private roomService: RoomService) {}

    public restorePlayerFromSession (roomId: string): Promise<Player> {
        const value = sessionStorage.getItem(roomId);
        if (value) {
            const player = JSON.parse(value);
            return this.roomService.getPlayersInRoom(roomId).pipe(take(1)).toPromise().then((players: Player[]) => {
                const isPlayerInRoom = players.some((roomPlayer: Player) => roomPlayer.id === player.id);
                if (isPlayerInRoom) {
                    return player;
                } else {
                    return null;
                }
            });
        } else {
            return Promise.resolve(null);
        }
    }

    public setPlayerForRoom (roomId: string, player: Player): void {
        sessionStorage.setItem(roomId, JSON.stringify(player));
    }

    public getDefaultPlayerName () {
        return localStorage.getItem('defaultPlayerName');
    }

    public setDefaultPlayerName (playerName: string) {
        localStorage.setItem('defaultPlayerName', playerName);
    }
}