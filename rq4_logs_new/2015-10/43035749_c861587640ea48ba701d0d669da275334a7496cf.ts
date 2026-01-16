/// <reference path="../../typings/tsd.d.ts" />

/// <reference path="../api.d.ts" />

import _ = require('lodash');
import async = require('async');

import {RoomEventController} from '../services/GameService';
import {DataStoreInterface, PlayerState} from '../datastore/DataStoreInterfaces';
import {RoomRouteControllerInterface} from './Routes';

import RouteErrors = require('./RouteErrors');

class RoomRouteController implements RoomRouteControllerInterface {
    private api:DataStoreInterface = null;
    private service:RoomEventController = null;

    constructor(api:DataStoreInterface, service:RoomEventController) {
        this.api = api;
        this.service = service;
    }

    getRoom(roomId:string, callback:(err:Error, room:ApiResponses.RoomResponse)=>any):any {
        async.auto({
            'game': [(autoCb, results) => this.api.room.getGame(roomId, autoCb)],
            'players': [(autoCb, results) => this.api.room.getPlayers(roomId, autoCb)]
        }, (err, results) => {
            if (err) {
                return callback(err, null);
            }

            var response:ApiResponses.RoomResponse = {
                room_id: roomId,
                game_id: results.game,
                players: results.players
            };

            callback(null, response);
        });
    }

    getRooms(callback:(err:Error, rooms:ApiResponses.RoomResponse[])=>any):any {

        async.auto({
            'roomIds': (autoCb, results) => this.api.room.getRooms(autoCb),
            'rooms': ['roomIds', (autoCb, results) => {
                async.mapLimit(results.roomIds, 10, (roomId:string, mapCb) => {
                    this.getRoom(roomId, mapCb)
                }, autoCb);
            }]
        }, (err, results) => {
            if (err) {
                return callback(err, null);
            }

            callback(null, results.rooms);
        });
    }

    postPlayer(roomId:string, player:string, callback:(err:Error, player:string)=>any):any {
        if (player !== 'player') {
            return callback(new Error(RouteErrors.ERROR_INVALID_PLAYER), null);
        }
        this.api.room.putPlayer(roomId, player, callback);
    }

    postGame(roomId:string, callback:(err:Error, game:ApiResponses.GameResponse)=>any):any {
        async.auto({
            'game': [(autoCb, results) => this.api.room.getGame(roomId, autoCb)],
            'players': [(autoCb, results) => this.api.room.getPlayers(roomId, autoCb)],
            'states': ['game', (autoCb, results) => this.api.game.getPlayerStates(results.game, autoCb)],
            'new_game': ['game', 'players', (autoCb, results) => {
                var gameEnded = this.service.isGameEnded(results.states);

                if (results.game && !gameEnded) {
                    // TODO reset game -- mark game as a loss/quit? detect "ended" game?
                    return autoCb(new Error(RouteErrors.ERROR_GAME_EXISTS), null);
                }

                if (!results.players || !results.players.length) {
                    return autoCb(new Error(RouteErrors.ERROR_MISSING_PLAYER), null);
                }

                this.api.game.postGame(autoCb);
            }],
            'assignGame': ['new_game', (autoCb, results) => {
                return this.api.room.setGame(roomId, results.new_game, autoCb);
            }],
            'shuffle': ['new_game', (autoCb, results) => {
                this.service.handleShuffle(results.new_game, autoCb);
            }],
            'player_states': ['players', 'new_game', 'shuffle', (autoCb, results) => {
                var players = results.players.concat('dealer');

                return autoCb(null, _.map(players, (player) => {
                    return {
                        player: player,
                        state: 'deal'
                    };
                }));
            }],
            'set_states': ['player_states', (autoCb, results) => {
                async.eachLimit<PlayerState>(results.player_states, 3, (state, eachCb) => {
                    this.api.game.setPlayerState(results.new_game, state.player, state.state, eachCb)
                }, autoCb);
            }]
        }, (err, results:any) => {
            if (err) {
                return callback(err, null);
            }

            var players:{[name:string]:ApiResponses.GamePlayerResponse} = {};

            _.forEach<{player:string; state:string}>(results.player_states, (value, key) => {
                players[value.player] = {
                    state: value.state,
                    cards: []
                }
            });

            var game:ApiResponses.GameResponse = {
                id: results.new_game,
                players: players,
                ended: false
            };

            callback(null, game);
        });
    }
}

export = RoomRouteController