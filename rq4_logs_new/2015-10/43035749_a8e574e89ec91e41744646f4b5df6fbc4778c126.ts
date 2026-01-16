/// <reference path="../../typings/tsd.d.ts" />

import data_driven = require("data-driven");
import should = require("should");
import sinon = require("sinon");

import DataStore = require('../datastore/DataStore');

import DataStoreModule = require('../datastore/DataStoreInterfaces');
import DataStoreInterface = DataStoreModule.DataStoreInterface;

import {GameServiceController} from './GameService';

describe('GameService', () => {
    var dataStore:DataStoreInterface = null;
    var gameService:GameServiceController = null;

    beforeEach((done) => {
        dataStore = DataStore.create();
        dataStore.connect(done);

        gameService = new GameServiceController(dataStore);
    });

    afterEach((done) => {
        dataStore.reset(done);
    });

    describe('getWinners', () => {
        data_driven([
            {
                label: 'dealer wins if everyone is bust',
                states: [{player: 'player', state: 'bust'}, {player: 'dealer', state: 'bust'}],
                scores: {'player': 22, 'dealer': 23},
                expected: ['dealer']
            },
            {
                label: 'dealer wins with higher score',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'stay'}],
                scores: {'player': 16, 'dealer': 20},
                expected: ['dealer']
            },
            {
                label: 'dealer wins with equal score',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'stay'}],
                scores: {'player': 20, 'dealer': 20},
                expected: ['dealer']
            },
            {
                label: 'player wins with higher score',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'stay'}],
                scores: {'player': 20, 'dealer': 19},
                expected: ['player']
            },
            {
                label: 'player wins if dealer busts',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'bust'}],
                scores: {'player': 20, 'dealer': 23},
                expected: ['player']
            },
            {
                label: 'dealer wins if player busts',
                states: [{player: 'player', state: 'bust'}, {player: 'dealer', state: 'stay'}],
                scores: {'player': 23, 'dealer': 20},
                expected: ['dealer']
            },
            {
                label: 'multiple players can win',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'stay'}, {player: 'other', state: 'stay'}],
                scores: {'player': 20, 'dealer': 19, 'other': 20},
                expected: ['player', 'other']
            },
            {
                label: 'survives empty states',
                states: [],
                scores: {'player': 20, 'dealer': 19, 'other': 20},
                expected: ['dealer']
            },
            {
                label: 'survives null states',
                states: [],
                scores: {'player': 20, 'dealer': 19, 'other': 20},
                expected: ['dealer']
            },
            {
                label: 'survives empty scores',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'stay'}],
                scores: {},
                expected: ['dealer']
            },
            {
                label: 'survives null scores',
                states: [{player: 'player', state: 'stay'}, {player: 'dealer', state: 'stay'}],
                scores: null,
                expected: ['dealer']
            }
        ], () => {
            it('{label}', (context) => {
                var actual = gameService.getWinners(context.states, context.scores);
                should.exist(actual);
                actual.should.containDeep(context.expected);
            });
        });

    });
});