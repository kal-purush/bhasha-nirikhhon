/// <reference path="../../typings/tsd.d.ts" />

/// <reference path="../api.d.ts" />

import {ResultDataStoreInterface} from '../datastore/DataStoreInterfaces';
import {LeaderboardRouteControllerInterface} from './Routes';

import {sendErrorOrResult} from './RouteErrors';

class LeaderboardRouteController implements LeaderboardRouteControllerInterface {
    private api:ResultDataStoreInterface = null;

    constructor(api:ResultDataStoreInterface) {
        this.api = api;
    }

    public getPlayer(player:string, callback:(err:Error, leaderboard:ApiResponses.LeaderboardResponse)=>any) {
        this.api.getPlayerWins(player, (err:Error, wins:number) => {
            if (err) {
                return callback(err, null);
            }
            callback(null, {player: player, wins: wins});
        });
    }

    public getMostWins(start:number, end:number, callback:(err:Error, leaderboard:ApiResponses.LeaderboardResponse[])=>any) {
        this.api.getMostWins(start, end, (err:Error, leaderboard:ApiResponses.LeaderboardResponse[]) => {
            if (err) {
                return callback(err, null);
            }
            callback(null, leaderboard);
        });
    }
}

export = LeaderboardRouteController;