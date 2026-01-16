/// <reference path="../typings/tsd.d.ts" />

'use strict';
require('es6-shim');

import { createStore, combineReducers, applyMiddleware } from 'redux';
import { List } from 'immutable';

import * as actions from '../client/actions';
import reducers from '../client/reducers';
import { logger } from '../client/middleware';
import BinManager from '../client/Smart/BinManager';
import { BinData } from '../client/Dumb/Bin';

import * as chai from 'chai';
import * as chaiAsPromised from 'chai-as-promised';
chai.use(chaiAsPromised);

var expect = chai.expect;
var assert = chai.assert;

var createStoreWithMiddleware = applyMiddleware(
    logger
)(createStore);
var store = createStoreWithMiddleware(reducers);


describe('Synchronous Action', () => {

    it('SET_BINS', () => {

        var bins = List<BinData>([
            { id: 0, position: 0, type: 'Romain', imageURL: '/img/waste/Ameublement.svg', isAvailable: true, isPending: false },
            { id: 1, position: 1, type: 'Henri', imageURL: '/img/waste/Batteries.svg', isAvailable: true, isPending: false },
            { id: 2, position: 2, type: 'Micheline', imageURL: '/img/waste/Bois.svg', isAvailable: true, isPending: false },
            { id: 3, position: 3, type: 'Erika', imageURL: '/img/waste/Ecrans.svg', isAvailable: true, isPending: false },
            { id: 4, position: 4, type: 'David', imageURL: '/img/waste/Metaux.svg', isAvailable: true, isPending: false }
        ]);

        store.dispatch(actions.setBins(bins));

        return new Promise((resolve, reject) => {
            var binsInStore = store.getState().bins;
            expect(binsInStore.size).to.deep.equal(5);
            resolve();
        });

    });   

    it('ADD_BIN', () => {
        var bin = { id: 5, position: 5, type: 'Camille', imageURL: '/img/waste/Metaux.svg', isAvailable: true, isPending: false };

        store.dispatch(actions.addBin(bin));

        return new Promise((resolve, reject) => {
            var binsInStore = store.getState().bins;
            expect(binsInStore.size).to.deep.equal(6);
            resolve();
        });
    });

    it('SET_BIN_AVAILABILITY', () => {

        store.dispatch(actions.setBinAvailability(2, false));

        return new Promise((resolve, reject) => {
            var unavailableBin = store.getState().bins.get(2);
            expect(unavailableBin.isAvailable).to.be.false;
            resolve();
        });
    });

    it('SET_BIN_PENDING', () => {

        store.dispatch(actions.setBinPending(4, true));

        return new Promise((resolve, reject) => {
            var pendingBin = store.getState().bins.get(4);
            expect(pendingBin.isPending).to.be.true;
            resolve();
        });
    }); 

});