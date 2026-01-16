/// <reference path="../../typings/tsd.d.ts" />

/// <reference path="../api.d.ts" />

import _ = require('lodash');
import async = require('async');

import {GameDataStoreInterface} from '../datastore/DataStoreInterfaces';
import {GameServiceController, GameServiceInterface} from '../services/GameService'

import {GameRouteControllerInterface} from './Routes';

import RouteErrors = require('./RouteErrors');

class GameRouteController implements GameRouteControllerInterface {
    private api:GameDataStoreInterface = null;
    private service:GameServiceInterface = null;

    public constructor(api:GameDataStoreInterface, service:GameServiceInterface) {
        this.api = api;
        this.service = service;
    }

    public getGame(gameId:string, callback:(err:Error, game:ApiResponses.GameResponse)=>any):any {
        async.auto({
            'states': (autoCb, results) => this.api.getPlayerStates(gameId, autoCb),
            'players': ['states', (autoCb, results) => autoCb(null, _.pluck(results.states, 'player'))],
            'cards': ['players', (autoCb, results) => {
                async.mapLimit<string, string[]>(results.players, 3, (player, eachCb) => this.api.getPlayerCards(gameId, player, eachCb), autoCb);
            }]
        }, (err, results) => {
            if (err) {
                callback(err, null);
            }

            var players:{[name:string]:ApiResponses.GamePlayerResponse} = {};

            _.forEach<{player:string; state:string}>(results.states, (value, key) => {
                players[value.player] = {
                    state: value.state,
                    cards: results.cards[key],
                    score: this.service.valueForCards(results.cards[key])
                }
            });

            var game:ApiResponses.GameResponse = {
                id: gameId,
                players: players,
                ended: this.service.isGameEnded(results.states)
            };

            if (!game.ended) {
                var states = _.reject(results.states, {player: 'dealer'});
                states = _.reject(states, {'state': GameServiceController.PLAYER_STATES.STAY});
                states = _.reject(states, {'state': 'bust'});
                if (states.length) {
                    players['dealer'].score = null;
                    if (players['dealer'].cards.length > 1) {
                        players['dealer'].cards[0] = 'XX';
                    }
                }
            }

            callback(null, game);
        });
    }

    public getCurrentTurn(gameId:string, callback:(err:Error, currentTurn:ApiResponses.GameCurrentTurnResponse)=>any):any {
        this.api.getPlayerStates(gameId, (err:Error, states:{player:string; state:string}[]) => {
            if (err) {
                callback(err, null);
            }

            var player = _.find<{player:string; state:string}>(states, "state", GameServiceController.PLAYER_STATES.CURRENT).player;
            var actions = [GameServiceController.PLAYER_ACTIONS.HIT, GameServiceController.PLAYER_ACTIONS.STAY];
            if (!player) {
                player = _.find<{player:string; state:string}>(states, "player", GameServiceController.PLAYER_STATES.DEALING).player;
                actions = [GameServiceController.PLAYER_ACTIONS.DEAL];
            }

            callback(null, {
                player: player, actions: actions
            });
        });
    }

    public postAction(gameId:string, player:string, action:string, callback:(err:Error)=>any):any {
        this.api.getPlayerStates(gameId, (err:Error, states:{player:string; state:string}[]) => {
            if (err) {
                callback(err);
            }

            var playerstate = _.find<{player:string; state:string}>(states, "player", player);
            if (!playerstate) {
                return callback(new Error(RouteErrors.ERROR_INVALID_PLAYER));
            }

            var state = playerstate.state;
            console.log("TODO: Do something", {action: action, state: state});

            if (action === GameServiceController.PLAYER_ACTIONS.HIT) {
                if (state !== GameServiceController.PLAYER_STATES.CURRENT) {
                    return callback(new Error(RouteErrors.ERROR_INVALID_TURN));
                }
                return this.api.rpoplpush(gameId, player, callback);
            }

            if (action === GameServiceController.PLAYER_ACTIONS.STAY) {
                if (state !== GameServiceController.PLAYER_STATES.CURRENT) {
                    return callback(new Error(RouteErrors.ERROR_INVALID_TURN));
                }
                return this.api.setPlayerState(gameId, player, GameServiceController.PLAYER_STATES.STAY, callback);
            }

            return callback(new Error(RouteErrors.ERROR_INVALID_ACTION));
        });
    }
}

export = GameRouteController;