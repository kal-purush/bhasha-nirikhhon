import {_State, Inform} from "./inform";
import {Computer} from "../types/computer";
import ip from "ip";
import {config} from "../config";
import {Storage} from "../services/storage";
import {Database} from "sqlite3";
import {Collection} from "../services/collection";
import express from "express";

describe('Inform', () => {
    let storage: Storage;
    let database: Database;
    let inform: Inform;

    beforeAll(async () => {
        database = new Database(':memory:');
        storage = new Storage(database);
        const server = express();
        const computers = new Collection<Computer>();
        const me: Computer = { ip: ip.address(), port: config.port };

        inform = new Inform(computers, 5000, me, server, storage);

        const states = [
            {publicKey:'peter',balance: 123,sender: '1'},
            {publicKey:'peter',balance: 123,sender: '2'},
            {publicKey:'peter',balance: 132,sender: '3'},
            {publicKey:'peter',balance: 132,sender: '3'},
        ]

        for (let i = 0; i < states.length; i++){
            inform.helpers.vote(states[i])
        }
    });

    describe('vote', () => {

        it('votes on a state and picks the one with 2/3 votes to put in the database', async () => {
            expect(inform.helpers.majorityStates()[0].balance).toBe(123)
        });

        it('empties info from votes list when a state has been picked', async () => {
            await inform.helpers.handleStates(inform.helpers.majorityStates());
            expect(inform.helpers.majorityStates().length).toBe(0)
        });

    })

});