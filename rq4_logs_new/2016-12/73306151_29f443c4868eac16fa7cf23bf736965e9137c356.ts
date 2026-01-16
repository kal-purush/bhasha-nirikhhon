'use strict';

import mongoose = require('mongoose');
import {initAdmin} from './user.model';
// let tingodb = require('tingodb');

const DB_OPTION: number = 3;
const DB_PORT: number = 27017;
const RETRY_SECONDS: number = 5;

/**
 * This singleton class manages the connection to the database and keeps it up and running.
 */
export class DBService {

    private static instance: DBService = new DBService();
    private static hostname: string;

    private db: mongoose.Connection = mongoose.connection;
    private dbLocation: any;

    private constructor() {
    }

    public static init(hostname: string): DBService {
        DBService.hostname = hostname;

        switch (DB_OPTION) {
            case 1:
                DBService.instance.dbLocation = `${DBService.hostname}:${DB_PORT}/homeautomation`;
                break;
            case 2:
            // not yet provided: new tingodb.Db('./data-tingodb', {});
            // break;
            default:
                DBService.instance.dbLocation = 'mongodb://admin:hallihallo62@ds050879.mlab.com:50879/homeautomation';
        }

        DBService.instance.db
            .once('open', () => console.log('DB ready'))
            .on('connecting', () => console.log('DB connecting...'))
            .on('connected', () => console.log('DB connected'))
            .on('reconnected', () => console.log('DB reconnected'))
            .on('disconnected', () => console.log('DB disconnected'))
            .on('error', (error: any) => {
                console.error(`DB connection error: ${error}`);
                mongoose.disconnect((err) => {
                    if (err) {
                        console.error(`mongoose disconnect failed: ${err}`);
                    }
                    DBService.instance.connect(RETRY_SECONDS);
                });
            });

        DBService.instance.connect(0);

        // create admin user if not yet existing
        initAdmin();

        return DBService.instance;
    }

    private connect(delay: number): void {
        console.log(`Trying to connect the DB in ${delay} seconds ...`);
        setTimeout(() => mongoose.connect(DBService.instance.dbLocation, {server:{auto_reconnect:true}}), delay * 1000);
    }

}